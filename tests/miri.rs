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
            t.initialize_unfilled().copy_from_slice(&bytes);
            t.set_filled(bytes.len());

            t
        })
        .collect();

    // Finalize in reverse order to test re-ordering logic
    for ticket in tickets.into_iter().rev() {
        w.finalize_ticket(ticket).unwrap();
    }

    for (bytes2, bytes) in bytes_of_bytes.iter().rev().zip(r.iter()) {
        println!();
        assert_eq!(bytes.bytes(), bytes2);
    }
}

#[test]
fn keep_one_byte_empty() {
    let (w, _r) = atomic_byte_channel::new(16);
    let _t1 = w.try_reserve(8).unwrap();
    let _t2 = w.try_reserve(4).unwrap();
    assert!(w.try_reserve(4).is_none());
    assert!(w.try_reserve(8).is_none());
    assert!(w.try_reserve(4).is_none());
    let _t3 = w.try_reserve(3).unwrap();

    assert!(w.try_reserve(1).is_none());
    assert!(w.try_reserve(1).is_none());
    assert!(w.try_reserve(2).is_none());
}

#[test]
fn concurrent_1() {
    let chunk_len = 128;
    let (w, r) = atomic_byte_channel::new(chunk_len * 8);

    let bytes_of_bytes: Vec<Vec<u8>> = (0..512)
        .map(|i| {
            (0u8..chunk_len as u8)
                .map(|v| v.wrapping_add(i as u8))
                .collect()
        })
        .collect();
    let bytes_of_bytes2 = bytes_of_bytes.clone();

    let t2 = std::thread::spawn(move || {
        for (bytes2, bytes) in bytes_of_bytes2.iter().zip(r.iter()) {
            println!();
            assert_eq!(bytes.bytes(), bytes2);
        }
    });

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
