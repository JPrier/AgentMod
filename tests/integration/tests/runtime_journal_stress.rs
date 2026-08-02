//! Concurrent canonical-journal stress coverage across shared and isolated sessions.

use std::{
    collections::BTreeSet,
    path::Path,
    sync::{Arc, Barrier},
    thread,
};

use agentmod_runtime_dependency::journal::{
    DependencyAppendJournalRequest, DependencyDurability, DependencyScanJournalRequest,
    JournalDependencyError, JournalDependencyPort, JsonlJournalDependency,
};

const WRITERS: usize = 8;
const APPENDS_PER_WRITER: usize = 50;

#[test]
fn concurrent_cas_journal_append_preserves_one_complete_chain() {
    let temporary = tempfile::tempdir().expect("temporary root");
    let session = Arc::new(temporary.path().join("contended-session"));
    let barrier = Arc::new(Barrier::new(WRITERS));
    let mut workers = Vec::new();
    for writer in 0..WRITERS {
        let session = Arc::clone(&session);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            barrier.wait();
            for local_sequence in 0..APPENDS_PER_WRITER {
                let event_id = format!("writer-{writer:02}-event-{local_sequence:03}");
                loop {
                    let scan = JsonlJournalDependency
                        .scan(DependencyScanJournalRequest {
                            session_directory: session.as_ref().clone(),
                        })
                        .expect("verified concurrent scan");
                    let sequence =
                        u64::try_from(scan.records.len()).expect("bounded stress record count") + 1;
                    let expected_head_event_id =
                        scan.records.last().map(|record| record.event_id.clone());
                    let result = JsonlJournalDependency.append(DependencyAppendJournalRequest {
                        session_directory: session.as_ref().clone(),
                        sequence,
                        expected_head_event_id,
                        event_id: event_id.clone(),
                        event_json: format!(
                            r#"{{"writer":{writer},"local_sequence":{local_sequence}}}"#
                        )
                        .into_bytes(),
                        durability: DependencyDurability::Buffered,
                    });
                    match result {
                        Ok(_) => break,
                        Err(
                            JournalDependencyError::SequenceMismatch { .. }
                            | JournalDependencyError::HeadEventIdMismatch { .. },
                        ) => thread::yield_now(),
                        Err(error) => panic!("unexpected concurrent append failure: {error}"),
                    }
                }
            }
        }));
    }
    for worker in workers {
        worker.join().expect("stress writer");
    }

    let scan = JsonlJournalDependency
        .scan(DependencyScanJournalRequest {
            session_directory: session.as_ref().clone(),
        })
        .expect("final verified scan");
    assert_eq!(scan.records.len(), WRITERS * APPENDS_PER_WRITER);
    assert_eq!(
        scan.records
            .iter()
            .map(|record| record.event_id.as_str())
            .collect::<BTreeSet<_>>()
            .len(),
        WRITERS * APPENDS_PER_WRITER
    );
    for (index, record) in scan.records.iter().enumerate() {
        assert_eq!(
            record.sequence,
            u64::try_from(index).expect("bounded index") + 1
        );
        if index > 0 {
            assert_eq!(
                record.previous_checksum.as_deref(),
                Some(scan.records[index - 1].checksum.as_str())
            );
        }
    }
}

#[test]
fn many_independent_session_journals_remain_isolated() {
    const SESSIONS: usize = 32;
    const EVENTS_PER_SESSION: usize = 25;

    let temporary = tempfile::tempdir().expect("temporary root");
    let root = Arc::new(temporary.path().to_path_buf());
    let mut workers = Vec::new();
    for session_index in 0..SESSIONS {
        let root = Arc::clone(&root);
        workers.push(thread::spawn(move || {
            let session = root.join(format!("session-{session_index:02}"));
            append_independent_session(&session, session_index, EVENTS_PER_SESSION);
        }));
    }
    for worker in workers {
        worker.join().expect("independent session writer");
    }
    for session_index in 0..SESSIONS {
        let scan = JsonlJournalDependency
            .scan(DependencyScanJournalRequest {
                session_directory: root.join(format!("session-{session_index:02}")),
            })
            .expect("independent verified scan");
        assert_eq!(scan.records.len(), EVENTS_PER_SESSION);
        assert!(scan.records.iter().all(|record| {
            record
                .event_id
                .starts_with(&format!("session-{session_index:02}-"))
        }));
    }
}

fn append_independent_session(session: &Path, session_index: usize, event_count: usize) {
    let mut expected_head_event_id = None;
    for index in 0..event_count {
        let event_id = format!("session-{session_index:02}-event-{index:03}");
        JsonlJournalDependency
            .append(DependencyAppendJournalRequest {
                session_directory: session.to_owned(),
                sequence: u64::try_from(index).expect("bounded index") + 1,
                expected_head_event_id,
                event_id: event_id.clone(),
                event_json: format!(r#"{{"session":{session_index},"event":{index}}}"#)
                    .into_bytes(),
                durability: DependencyDurability::Buffered,
            })
            .expect("independent append");
        expected_head_event_id = Some(event_id);
    }
}
