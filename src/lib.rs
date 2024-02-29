//! An async, byte-orented, high-performance, mpmc channel,
//!
//! Use cases:
//! 1. One thread continuously generates IPV4 packets that are sent to multiple worker I/O threads
//!    for better throughput
//! 2. Multi threaded linker wants to create two separate sections in the same elf file that can be
//!    written to in parallel

#[cfg(not(feature = "std"))]
extern crate alloc;

pub mod prelude {
    pub(crate) use core::{cell::UnsafeCell, fmt::Debug, sync::atomic::Ordering};

    #[cfg(loom)]
    pub(crate) mod atomic {
        pub(crate) use atomic_waker::AtomicWaker;
        pub(crate) use loom::sync::atomic::{fence, AtomicBool, AtomicU8, AtomicUsize};
    }

    #[cfg(not(loom))]
    pub(crate) mod atomic {
        #[cfg(not(feature = "std"))]
        pub use alloc::{collections::VecDeque, vec::Vec};
        #[cfg(feature = "std")]
        pub use std::{collections::VecDeque, vec::Vec};

        pub(crate) use core::sync::atomic::{AtomicBool, AtomicUsize};
    }

    // Use std'd Arc even when under loom
    #[cfg(not(feature = "std"))]
    pub use alloc::sync::Arc;
    #[cfg(feature = "std")]
    pub use std::sync::Arc;

    pub(crate) use atomic::*;

    pub use super::{new, Reader, Writer};
}

use prelude::*;

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
    new_with_packet_estimate(capacity, 16)
}

pub fn new_with_packet_estimate(capacity: usize, average_packet_size: usize) -> (Writer, Reader) {
    let v: Vec<UnsafeCell<u8>> = (0..capacity).map(|_| UnsafeCell::new(0u8)).collect();

    let buf: Arc<[UnsafeCell<u8>]> = v.into_boxed_slice().into();

    let shared = Arc::new(Shared {
        head: CachePadded::new(AtomicUsize::new(0)),
        tail: CachePadded::new(AtomicUsize::new(0)),
    });
    let max_packets_in_flight = capacity / average_packet_size;
    let (ticket_tx, ticket_rx) = crossbeam_channel::bounded(max_packets_in_flight);

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

// After try_reserve(8) #1:
// len: 17
// Head [        |        ] 8
// Tail [|                ] 0
//
// After try_reserve(8) #2:
// Head [                |] 16
// Tail [|                ] 0
//
// After reader handles ticket(8) #1:
// Head [                |] 16
// Tail [        |        ] 0
//
// After reader handles ticket(8) #2:
// Head [                |] 16
// Tail [                |] 0

impl Writer {
    /// Tries to reserve a
    pub fn try_reserve(&self, len: usize) -> Option<Ticket<'_>> {
        let mut head = self.shared.head.load(Ordering::Acquire);
        loop {
            let tail = self.shared.tail.load(Ordering::Acquire);

            // Wraparound to front of buffer we don't have `len` bytes before the end of the buffer
            let wraparound = self.index(head) + len > self.buf.len();

            println!("try_reserve: head: {head}, tail: {tail}, len: {len}");
            if wraparound {
                // don't step on reader(s)
                if len > self.index(tail) {
                    // Not enough space at front of buffer
                    println!("Not enough space at front of buffer");
                    return None;
                }
                println!("using wraparound");
            } else {
                // don't step on reader(s)
                let end = head + len;
                if end > tail + self.buf.len() {
                    println!("Allocate wouldn't fit (without wraparound)");
                    return None;
                }
            }

            let new_head = if wraparound {
                next_mutiple(head, self.buf.len()) + len
            } else {
                head + len
            };

            // NOTE: we cant use `fetch_add`, since we need to prevent wraparound
            let offset = match self.shared.head.compare_exchange_weak(
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

            let idx = self.index(new_head - len);
            println!(
               "try_reserve: head {offset} -> {new_head}, making indices {}..{} available to writer",
               idx,
               idx + len
            );

            // SAFETY:
            // 1. Readers never read beyond `head`
            // 2. We atomically exchanged `head` with `new_head` atomically, giving unique access to
            //    the range `head..new_head`
            let start: *const UnsafeCell<u8> = unsafe { self.buf.as_ptr().add(idx) };
            // NOTE: currently this fails under miri since we are only creating a stacked borrow at
            // `buf[0]`, and not `buf[..]`. We need an api to turn &[UnsafeCell<T>] -> &[T] to
            // inform miri we are creating Unique borrows of all elements in `buf`, however this
            // doesn't currently exist. TODO: open pr in rust-lang/rust and maybe miri
            let start = unsafe { (*start).get() };

            // TODO: we also likely want to use `slice::slice_from_ptr_range`,
            // But this is currently unstable https://github.com/rust-lang/rust/issues/89792
            let buf = unsafe { core::slice::from_raw_parts_mut(start, len) };

            break Some(Ticket {
                buf: ReadBuf::new(buf),
                alloc_start: offset,
                alloc_end: new_head,
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

        println!("Buffered tickets: {}", self.ticket_tx.len());
        self.ticket_tx
            .try_send(FinalizedTicket {
                alloc_start: ticket.alloc_start,
                len: ticket.buf.capacity(),
                alloc_end: ticket.alloc_end,
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
    // TODO: fix
    #[allow(clippy::result_unit_err)]
    pub fn try_recv(&self) -> Result<PacketHandle<'_>, ()> {
        Ok(self.handle_recv(self.ticket_rx.try_recv().map_err(|_| ())?))
    }

    pub(crate) fn handle_recv(&self, ticket: FinalizedTicket) -> PacketHandle<'_> {
        self.try_run_cleanup();

        let wraparound = ticket.alloc_end - ticket.len != ticket.alloc_start;
        let idx = if wraparound {
            debug_assert!(self.index(ticket.alloc_end) == ticket.len);
            0
        } else {
            self.index(ticket.alloc_end - ticket.len)
        };
        let start: *const u8 = self.buf[idx].get();

        // SAFETY: We have received a finalized ticket from a writer, indicating that we have
        // exclusive access to the range ticket.offset..(ticket.offset + ticket.len)
        let buf = unsafe { core::slice::from_raw_parts(start, ticket.len) };
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
                match self.try_cleanup_ticket(ticket) {
                    Ok(()) => pending.pop_front(),
                    Err(()) => break,
                };
            }
            self.pending_empty
                .store(pending.is_empty(), Ordering::Release);
        }
    }

    pub(crate) fn add_to_pending_list(&self, ticket: FinalizedTicket) {
        // Awkward, this ticket was submitted out of order with respect to reserving memory,
        // Add to pending list and handle later, once tickets coming before this one are cleaned up
        let mut pending = self.pending_handles.lock();
        // keep `pending_handles` sorted by their offset fields, since we need to perform
        // cleanup in order
        let insert_index =
            match pending.binary_search_by(|e| e.alloc_start.cmp(&ticket.alloc_start)) {
                Ok(i) => i,
                Err(i) => i,
            };

        pending.insert(insert_index, ticket.clone());
        self.pending_empty.store(false, Ordering::Release);
        println!("Cant handle ticket currently. Cleaning {ticket:?} pending: {pending:?}");
    }

    pub(crate) fn try_cleanup_ticket(&self, ticket: &FinalizedTicket) -> Result<(), ()> {
        let tail = self.shared.tail.load(Ordering::Acquire);

        println!("Reader trying to cleanup {ticket:?}");
        if ticket.alloc_start > tail {
            return Err(());
        }

        match self.shared.tail.compare_exchange_weak(
            ticket.alloc_start,
            ticket.alloc_end,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => {
                println!(
                    "Reader handled ticket {ticket:?}, tail: {} -> {}",
                    ticket.alloc_start, ticket.alloc_end
                );
                Ok(())
            }
            Err(actual_tail) => {
                // Either:
                // ticket.offset > tail (handled above)
                // ticket.offset == tail, leads to Ok branch
                // Nobody else can increment tail if tail equals our ticket's offset
                unreachable!("Reader failed to increment tail after ticket recv. Read section {}..{}, but tail was actually {actual_tail}", ticket.alloc_start, ticket.alloc_end)
            }
        }
    }

    pub(crate) fn index(&self, offset: usize) -> usize {
        offset % self.buf.len()
    }

    pub fn iter(&self) -> ReaderIterator<'_> {
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

impl<'a> core::ops::Deref for PacketHandle<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buf
    }
}

impl Drop for PacketHandle<'_> {
    fn drop(&mut self) {
        if self.reader.try_cleanup_ticket(&self.ticket).is_err() {
            self.reader.add_to_pending_list(self.ticket.clone());
        }
    }
}

pub struct ReaderIterator<'a> {
    reader: &'a Reader,
}

impl<'a> Iterator for ReaderIterator<'a> {
    type Item = PacketHandle<'a>;

    #[cfg(not(loom))]
    fn next(&mut self) -> Option<Self::Item> {
        let ticket = self.reader.ticket_rx.recv().ok()?;
        Some(self.reader.handle_recv(ticket))
    }

    #[cfg(loom)]
    fn next(&mut self) -> Option<Self::Item> {
        let ticket = loop {
            // blocking recv deadlocks under loom since the model doesn't know were blocked on
            // another thread for progress
            match self.reader.ticket_rx.try_recv() {
                Ok(t) => break t,
                Err(_) => loom::hint::spin_loop(),
            }
        };
        Some(self.reader.handle_recv(ticket))
    }
}

#[derive(Clone, Debug)]
struct FinalizedTicket {
    alloc_start: usize,
    /// The (non-wrapping) location this allocation ends at
    /// NOTE: May be more than `offset_start + len` to handle wraparound
    /// NOTE: The user's data _always_ starts at `alloc_end` - len. When wraparound occurs,
    /// The user's first byte will be at `buf[0]`, therefore: `self.index(alloc_end) == len`
    alloc_end: usize,
    len: usize,
}

#[derive(Debug)]
pub struct Ticket<'a> {
    buf: ReadBuf<'a>,
    /// The (non-wrapping) location this ticket starts at
    alloc_start: usize,
    /// The (non-wrapping) location this ticket ends at
    /// NOTE: may be more than `offset_start + len` to handle wraparound
    alloc_end: usize,
}

impl<'a> core::ops::Deref for Ticket<'a> {
    type Target = ReadBuf<'a>;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl<'a> core::ops::DerefMut for Ticket<'a> {
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
//                 Ticket A | Ticket B | No active tickets
// Ring buffer: [ XXXX......|XXXXX.....|............... ]
//                ^tail                ^head
// (Ticket A owned by Thread A, and Ticket B owned by Thread B)
//
// Until the writers complete their tickets, tail will remain at 0, and readers will
// be unable to read the data until finalized.
//
// Now Thread B finishes writing its data and sends the ticket:
//                 Ticket A | Ticket B | No active tickets
// Ring buffer: [ XXXXXX....|XXXXXXXXXX|............... ]
//                ^tail                ^head
//
// Like a normal ring buffer, we want to advance the write pointer when data is finished being written.
// If we naively do this in the situation presented above, the readers will break aliasing rules by
// reading Thread A's partially complete write.
//
// The solution is to use a channel between the readers and the writers. Once any writer is done
// with a ticket, we send it over the channel, letting a reader read the ticket's bytes
// immediately.
// Once the reader is done with a ticket, it tries to advance tail (to unblock the writer), but
// in the example above, we can only advance tail if Ticket A is received first.
//
// Therefore, we store information about Ticket B into a VecDeque shared by the readers. Once
// Ticket A is done being read, we will find Ticket B in this list and free both A and B at the
// same time, allowing us to advance tail, giving more space to the writer.
//
// After Ticket B is released by a reader:
//                                     | Ticket C | ...
// Ring buffer: [ .....................|............... ]
//                                     ^head
//                                     ^tail
// Now the writer(s) have the entire buffer to use
