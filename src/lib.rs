//! An async, byte-orented, high-performance, mpmc channel,
//!
//! Use cases:
//! 1. One thread continuously generates IPV4 packets that are sent to multiple worker I/O threads
//!    for better throughput
//! 2. Multi threaded linker wants to create two separate sections in the same elf file that can be
//!    written to in parallel

use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

// TODO: switch to our own
// tokio has a decent one for what we need, so just use that for now
use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
pub use tokio::io::ReadBuf;

pub struct Shared {
    /// Offset of the next available byte for writing, modulo the size of the buffer
    /// Used for atomically coordinating unique access to parts of the buffer by writers
    ///
    // NOTE: this and `tail` are offsets not indices, to prevent the ABA problem.
    // Take this modulo the buffer's size before accessing anything in the buffer
    // `head == tail` indicates that the buffer is empty, as we always leave at least one byte open
    head: CachePadded<AtomicUsize>,

    /// Offset of the oldest element not fully consumed by readers (write limit for ring buffer),
    /// modulo the size of the buffer
    tail: CachePadded<AtomicUsize>,
}

pub fn new(capacity: usize) -> (Writer, Reader) {
    let v: Vec<UnsafeCell<u8>> = (0..capacity).map(|_| UnsafeCell::new(0u8)).collect();
    let buf: Arc<[UnsafeCell<u8>]> = v.into_boxed_slice().into();

    let shared = Arc::new(Shared {
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
    });
    let (ticket_tx, ticket_rx) = crossbeam_channel::bounded(8);

    (
        Writer {
            buf: Arc::clone(&buf),
            shared: Arc::clone(&shared),
            ticket_tx,
        },
        Reader {
            buf,
            shared,
            ticket_rx,
            pending_handles: Arc::new(Mutex::new(VecDeque::new())),
            pending_empty: Arc::new(AtomicBool::new(true)),
        },
    )
}

unsafe impl Send for Writer {}
unsafe impl Sync for Writer {}

#[derive(Clone)]
pub struct Writer {
    // TODO: bench perf of making uninitialized and compare to now
    buf: Arc<[UnsafeCell<u8>]>,
    shared: Arc<Shared>,
    ticket_tx: Sender<FinalizedTicket>,
}

/// Returns `n` rounded up to the nearest multiple of `base`.
/// If `n` is already a multiple of `base`, `n` is unchanged
fn next_mutiple(n: usize, base: usize) -> usize {
    let one_minus_base = base - 1;
    (n + one_minus_base) / base * base
}

impl Writer {
    /// Tries to reserve a
    pub fn try_reserve<'a>(&'a self, len: usize) -> Option<Ticket<'a>> {
        let mut head = self.shared.head.load(Ordering::Acquire);
        loop {
            let tail = self.shared.tail.load(Ordering::Acquire);
            // TODO: ensure we can find `len` continuous bytes

            // Ensure new head doesnt step on the reader's tail
            let bytes_used = head - tail;
            let bytes_remaining = self.buf.len() - bytes_used;
            if len >= bytes_remaining {
                // Use >= to always keep one byte free for distinguishing empty from full
                return None;
            }

            let start_index = self.index(head);
            let mut new_head = head + len;
            if start_index + len > self.buf.len() {
                dbg!(start_index, head, self.buf.len());
                // prevent wraparound since we must give contiguous memory out
                new_head = next_mutiple(head, self.buf.len()) + len;
            }

            // NOTE: we cant use `fetch_add`, since we need to prevent wraparound
            let offset = match self.shared.head.compare_exchange(
                head,
                new_head,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(offset) => offset,
                Err(new_head) => {
                    head = new_head;
                    continue;
                }
            };

            let start = self.buf[self.index(offset)].get();

            // SAFETY:
            // 1. Readers never read bytes
            // 2. We exchanged `head` with `new_head` atomically, giving unique access to `len` bytes
            //    starting at `offest`
            let buf = unsafe { std::slice::from_raw_parts_mut(start, len) };

            break Some(Ticket {
                buf: ReadBuf::new(buf),
                offset,
            });
        }
    }

    pub fn finalize_ticket<'a>(&'a self, ticket: Ticket<'a>) -> Result<(), Ticket<'a>> {
        let filled = ticket.buf.filled().len();
        let capacity = ticket.buf.capacity();
        // TODO: do we want to support the user changing the length?
        if filled != capacity {
            panic!("Ticket not completely filled with data! Reserved {capacity} bytes, but user only wrote {filled} bytes");
        }

        self.ticket_tx
            .try_send(FinalizedTicket {
                offset: ticket.offset,
                len: ticket.buf.capacity(),
            })
            .map_err(|_| ticket)
    }

    pub(crate) fn index(&self, offset: usize) -> usize {
        offset % self.buf.len()
    }
}

unsafe impl Send for Reader {}
unsafe impl Sync for Reader {}

#[derive(Clone)]
pub struct Reader {
    buf: Arc<[UnsafeCell<u8>]>,
    shared: Arc<Shared>,
    ticket_rx: Receiver<FinalizedTicket>,
    /// Handles that have been dropped, but cant be handled since they were received out of order
    pending_handles: Arc<Mutex<VecDeque<FinalizedTicket>>>,
    // `pending_handles.is_empty()` in an atomic (best effort)
    pending_empty: Arc<AtomicBool>,
    // TODO: add atomic bool for flagging that no handles are pending (no re-order needed)
}

impl Reader {
    pub fn try_recv<'a>(&'a self) -> Result<PacketHandle<'a>, ()> {
        Ok(self.handle_recv(self.ticket_rx.try_recv().map_err(|_| ())?))
    }

    pub(crate) fn handle_recv<'a>(&'a self, ticket: FinalizedTicket) -> PacketHandle<'a> {
        self.try_run_cleanup();
        println!("Reader got ticket: {ticket:?}");

        let index = self.index(ticket.offset);
        let start: *const u8 = self.buf[index].get();

        // SAFETY: We have received a finalized ticket from a writer, indicating that we have
        // exclusive access to the range ticket.offset..(ticket.offset + ticket.len)
        let buf = unsafe { std::slice::from_raw_parts(start, ticket.len) };
        PacketHandle {
            buf,
            ticket,
            reader: self,
        }
    }

    pub(crate) fn try_run_cleanup(&self) {
        if self.pending_empty.load(Ordering::Acquire) {
            return;
        }

        if let Some(mut pending) = self.pending_handles.try_lock() {
            println!("{} tickets remain unhandled", pending.len());
            // Tickets sorted, so if we are unable to cleanup one, we will be unable to cleanup
            // anything after it
            while let Some(ticket) = pending.front() {
                match self.try_cleanup_ticket(ticket, false) {
                    Ok(()) => pending.pop_front(),
                    Err(()) => break,
                };
            }
            self.pending_empty
                .store(pending.is_empty(), Ordering::Release);
        }
    }

    pub(crate) fn try_cleanup_ticket<'a>(
        &'a self,
        ticket: &FinalizedTicket,
        add_if_pending: bool,
    ) -> Result<(), ()> {
        let tail = self.shared.tail.load(Ordering::Acquire);

        println!("Trying to cleanup {ticket:?}");
        if ticket.offset > tail {
            if add_if_pending {
                // Awkward, this ticket was submitted out of order with respect to reserving memory,
                // Add to pending list and handle later, once tickets coming before this one are cleaned up
                let mut pending = self.pending_handles.lock();
                // keep `pending_handles` sorted by their offset fields, since we need to perform
                // cleanup in order
                let insert_index = match pending.binary_search_by(|e| e.offset.cmp(&ticket.offset))
                {
                    Ok(i) => i,
                    Err(i) => i,
                };
                pending.insert(insert_index, ticket.clone());
                self.pending_empty
                    .store(pending.is_empty(), Ordering::Release);
                dbg!(&pending);
                println!("Cant handle ticket currently. Cleaning {ticket:?} later tail: {tail}");
            }
            return Err(());
        }

        loop {
            // New tail index (if we just received the oldest ticket)
            let new_tail = ticket.offset + ticket.len;
            // TODO: handle case where reserved size is too big and wraparound occurs, causing
            // spacing at the end of the buffer between packets

            match self.shared.tail.compare_exchange(
                ticket.offset,
                new_tail,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    println!("Handled ticket {ticket:?}");
                    break Ok(());
                }
                Err(actual_tail) => {
                    // Either:
                    // ticket.offset > tail (handled above)
                    // ticket.offset == tail, leads to Ok branch
                    // Nobody else can increment tail if tail equals our ticket's offset
                    unreachable!("Reader failed to increment tail after ticket recv. Read section {}..{new_tail}, but tail was actually {actual_tail}", ticket.offset)
                }
            }
        }
    }

    pub(crate) fn index(&self, offset: usize) -> usize {
        offset % self.buf.len()
    }

    pub fn iter<'a>(&'a self) -> ReaderIterator<'a> {
        ReaderIterator { reader: self }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.try_run_cleanup();
    }
}

pub struct PacketHandle<'a> {
    buf: &'a [u8],
    ticket: FinalizedTicket,
    reader: &'a Reader,
}

impl PacketHandle<'_> {
    pub fn bytes(&self) -> &[u8] {
        self.buf
    }
}

impl<'a> std::ops::Deref for PacketHandle<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl Drop for PacketHandle<'_> {
    fn drop(&mut self) {
        let _ = self.reader.try_cleanup_ticket(&self.ticket, true);
    }
}

pub struct ReaderIterator<'a> {
    reader: &'a Reader,
}

impl<'a> Iterator for ReaderIterator<'a> {
    type Item = PacketHandle<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let ticket = self.reader.ticket_rx.recv().ok()?;
        Some(self.reader.handle_recv(ticket))
    }
}

#[derive(Clone, Debug)]
struct FinalizedTicket {
    /// The (non-wrapping) index this ticket starts at
    offset: usize,
    len: usize,
}

#[derive(Debug)]
pub struct Ticket<'a> {
    buf: ReadBuf<'a>,
    /// The (non-wrapping) index this ticket starts at
    offset: usize,
}

impl<'a> std::ops::Deref for Ticket<'a> {
    type Target = ReadBuf<'a>;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl<'a> std::ops::DerefMut for Ticket<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

// TLDR: How to make mpmc work efficiently
//
// The secret sauce of this crate is that writers can call reserve in parallel, and receive a
// mutable slice in the ring buffer that they can write to directly into (via a ticket).
// Because these slices are non-overlapping, and because the readers can't read them, the writers
// are able to write in parallel.
//
// The issue comes when writers finish with their ticket in different orders than they requested
// them.
//
// Image the following scenario:
// Two writers call `try_reserve` around the same time and both succeed. They begin writing their
// data to the ring buffer:
//                 Writer A | Writer B | No active tickets
// Ring buffer: [ XXX.......|XXXX......|............... ]
// Until the writers complete their tickets, any reader's pointer will stay at zero, and readers will
// be unable to read the data until finalized.
//
// Now Writer B finishes writing its data and sends the ticket:
//                 Writer A | Writer B | No active tickets
// Ring buffer: [ XXXXX.....|XXXXXXXXXX|............... ]
// Like a normal ring buffer, we advance the write pointer when data is finished being written.
// If we naively do this in the situation presented above, the readers will break aliasing rules by
// reading Writer A's partially complete write.
//
// We could use a channel to get around this, by sending the finished range to readers, and only
// using the ring buffer's points for allocating chunks in `try_reserve` and keeping the readers
// separate from the writers, but this feels unsatisfying.
//
// EDIT: I will do the channel implementation first since its simpler, then bench / evaluate
// against a more proper implementation once I can solve this problem. The channel solution also
// has a similar problem when packets are finished being read in a different out of order. We have
// to maintain a list of pending packets that will be handled once packets before it are finished
// being read
