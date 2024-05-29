#![cfg(loom)]

/*
use std::sync::{atomic::Ordering, Arc};

macro_rules! loom_async_model {
    ($block:expr) => {
        loom::model(|| loom::future::block_on($block));
    };
}

fn loom_spawn<F>(f: F) -> loom::thread::JoinHandle<()>
where
    F: futures::Future<Output = ()> + 'static,
{
    loom::thread::spawn(move || {
        loom::future::block_on(f);
    })
}
*/

#[test]
fn concurrent_1() {
    let mut i = 0;
    let bytes_of_bytes: &'static Vec<Vec<u8>> = Box::leak(Box::new(
        (0..3)
            .map(|_| {
                (0..8)
                    .map(|_| {
                        i += 1;
                        i
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
    ));

    loom::model(move || {
        let (w, r) = atomic_byte_channel::new(17);

        let t2 = loom::thread::spawn(move || {
            for (bytes2, bytes) in bytes_of_bytes.iter().zip(r.iter()) {
                assert_eq!(bytes.bytes(), bytes2);
            }
            println!("DONE");
        });

        for bytes in bytes_of_bytes {
            let mut ticket = loop {
                if let Some(t) = w.try_reserve(bytes.len()) {
                    break t;
                } else {
                    loom::hint::spin_loop();
                }
            };
            ticket.initialize_unfilled().copy_from_slice(&bytes);
            println!("cap: {}", ticket.capacity());
            ticket.set_filled(bytes.len());
            println!("filled: {}", ticket.filled().len());

            loop {
                match w.finalize_ticket(ticket) {
                    Ok(()) => break,
                    Err(t) => {
                        println!("Writer failed to finalize ticket - internal buffer out of space");
                        loom::hint::spin_loop();
                        ticket = t
                    }
                }
            }
        }

        t2.join().unwrap();
    });
}
