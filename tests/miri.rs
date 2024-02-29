#![cfg(not(loom))]

use std::sync::{Arc, Barrier};

#[test]
fn feels_good() {
    let chunk_len = 128;
    let (w, r) = atomic_byte_channel::new(8 * chunk_len);
    for i in 0..64 {
        let bytes: Vec<u8> = (0u8..chunk_len as u8).map(|v| v.wrapping_mul(i)).collect();
        let mut t = w.try_reserve(chunk_len).unwrap();
        t.initialize_unfilled().copy_from_slice(&bytes);
        t.set_filled(chunk_len);
        w.finalize_ticket(t).unwrap();

        let packet = r.try_recv().unwrap();
        assert_eq!(&bytes, packet.bytes());
    }
}

#[test]
fn recv_out_of_order() {
    let chunk_len = 128;
    let (w, r) = atomic_byte_channel::new(chunk_len * 8);

    let bytes_of_bytes: Vec<Vec<u8>> = (0..7)
        .map(|i| (0u8..chunk_len as u8).map(|v| v.wrapping_add(i)).collect())
        .collect();

    let tickets: Vec<_> = bytes_of_bytes
        .iter()
        .map(|bytes| {
            let mut t = w.try_reserve(chunk_len).unwrap();
            t.initialize_unfilled().copy_from_slice(bytes);
            t.set_filled(bytes.len());

            t
        })
        .collect();

    // Finalize in reverse order to test re-ordering logic
    for ticket in tickets.into_iter().rev() {
        w.finalize_ticket(ticket).unwrap();
    }

    for (bytes2, bytes) in bytes_of_bytes.iter().rev().zip(r.iter()) {
        assert_eq!(bytes.bytes(), bytes2);
    }
}

#[test]
fn concurrent_1() {
    let chunk_len = 32;
    let (w, r) = atomic_byte_channel::new(chunk_len * 16);

    let barrier1 = Arc::new(Barrier::new(2));
    let barrier2 = Arc::clone(&barrier1);

    let bytes_of_bytes: Vec<Vec<u8>> = (0..2048)
        .map(|i| {
            (0u8..chunk_len as u8)
                .map(|v| v.wrapping_add(i as u8))
                .collect()
        })
        .collect();
    let bytes_of_bytes2 = bytes_of_bytes.clone();

    let t2 = std::thread::spawn(move || {
        barrier1.wait();

        for (bytes2, bytes) in bytes_of_bytes2.iter().zip(r.iter()) {
            assert_eq!(bytes.bytes(), bytes2);
        }
    });

    // Release the hounds at the same time!
    barrier2.wait();

    for bytes in bytes_of_bytes {
        let mut ticket = loop {
            if let Some(t) = w.try_reserve(chunk_len) {
                break t;
            }
        };
        ticket.initialize_unfilled().copy_from_slice(&bytes);
        ticket.set_filled(bytes.len());

        loop {
            match w.finalize_ticket(ticket) {
                Ok(()) => break,
                Err(t) => ticket = t,
            }
        }
    }

    t2.join().unwrap();
}

#[test]
fn concurrent_lengths() {
    let (w, r) = atomic_byte_channel::new(33);
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let mut i = 0usize;
    let bytes_of_bytes: Vec<Vec<u8>> = (0..2048)
        .map(|_| {
            (0..rng.gen_range(0..8))
                .map(|_| {
                    i += 1;
                    i as u8
                })
                .collect()
        })
        .collect();
    let bytes_of_bytes2 = bytes_of_bytes.clone();

    let t2 = std::thread::spawn(move || {
        for (bytes2, bytes) in bytes_of_bytes2.iter().zip(r.iter()) {
            assert_eq!(bytes.bytes(), bytes2);
        }
    });

    for bytes in bytes_of_bytes {
        // Spin if we cant allocate a ticket (want to test core functionality, async/blocking api
        // tested elsewhere)
        let mut ticket = loop {
            if let Some(t) = w.try_reserve(bytes.len()) {
                break t;
            }
        };
        ticket.initialize_unfilled().copy_from_slice(&bytes);
        ticket.set_filled(bytes.len());

        loop {
            match w.finalize_ticket(ticket) {
                Ok(()) => break,
                Err(t) => {
                    ticket = t;
                }
            }
        }
    }

    t2.join().unwrap();
}
