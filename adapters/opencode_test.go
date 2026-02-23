package adapters

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestOpencodeAdapterSQLiteSessions(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	projectOne := filepath.Join(tempHome, "work", "project-one")
	projectTwo := filepath.Join(tempHome, "work", "project-two")

	dbPath := filepath.Join(tempHome, ".local", "share", "opencode", "opencode.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		t.Fatalf("failed to create db directory: %v", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	if _, err := db.Exec(`
		CREATE TABLE project (
			id TEXT PRIMARY KEY,
			worktree TEXT NOT NULL,
			vcs TEXT,
			name TEXT,
			icon_url TEXT,
			icon_color TEXT,
			time_created INTEGER NOT NULL,
			time_updated INTEGER NOT NULL,
			time_initialized INTEGER,
			sandboxes TEXT NOT NULL,
			commands TEXT
		);
		CREATE TABLE session (
			id TEXT PRIMARY KEY,
			project_id TEXT NOT NULL,
			parent_id TEXT,
			slug TEXT NOT NULL,
			directory TEXT NOT NULL,
			title TEXT NOT NULL,
			version TEXT NOT NULL,
			share_url TEXT,
			summary_additions INTEGER,
			summary_deletions INTEGER,
			summary_files INTEGER,
			summary_diffs TEXT,
			revert TEXT,
			permission TEXT,
			time_created INTEGER NOT NULL,
			time_updated INTEGER NOT NULL,
			time_compacting INTEGER,
			time_archived INTEGER
		);
		CREATE TABLE message (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL,
			time_created INTEGER NOT NULL,
			time_updated INTEGER NOT NULL,
			data TEXT NOT NULL
		);
		CREATE TABLE part (
			id TEXT PRIMARY KEY,
			message_id TEXT NOT NULL,
			session_id TEXT NOT NULL,
			time_created INTEGER NOT NULL,
			time_updated INTEGER NOT NULL,
			data TEXT NOT NULL
		);
	`); err != nil {
		t.Fatalf("failed to create sqlite schema: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO project (id, worktree, vcs, name, time_created, time_updated, sandboxes)
		VALUES
			('proj_one', ?, 'git', 'project-one', 1000, 1000, '[]'),
			('proj_two', ?, 'git', 'project-two', 1000, 1000, '[]');
	`, projectOne, projectTwo); err != nil {
		t.Fatalf("failed to insert projects: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated)
		VALUES
			('ses_one', 'proj_one', 'session-one', ?, 'SQLite session one', '1.2.2', 2000, 2100),
			('ses_two', 'proj_two', 'session-two', ?, 'SQLite session two', '1.2.2', 3000, 3100);
	`, projectOne, projectTwo); err != nil {
		t.Fatalf("failed to insert sessions: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO message (id, session_id, time_created, time_updated, data)
		VALUES
			('msg_user', 'ses_one', 2010, 2010, '{"role":"user","time":{"created":2010}}'),
			('msg_assistant', 'ses_one', 2020, 2025, '{"role":"assistant","time":{"created":2020},"modelID":"gpt-5.3-codex","mode":"codex-5.3","tokens":{"input":10,"output":20}}'),
			('msg_user_2', 'ses_two', 3010, 3010, '{"role":"user","time":{"created":3010}}');
	`); err != nil {
		t.Fatalf("failed to insert messages: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO part (id, message_id, session_id, time_created, time_updated, data)
		VALUES
			('part_user', 'msg_user', 'ses_one', 2011, 2011, '{"type":"text","text":"How do I fix this?"}'),
			('part_assistant', 'msg_assistant', 'ses_one', 2021, 2021, '{"type":"text","text":"Use SQLite fallback."}'),
			('part_user_2', 'msg_user_2', 'ses_two', 3011, 3011, '{"type":"text","text":"Another session"}');
	`); err != nil {
		t.Fatalf("failed to insert parts: %v", err)
	}

	adapter, err := NewOpencodeAdapter()
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	sessions, err := adapter.ListSessions("", 10)
	if err != nil {
		t.Fatalf("ListSessions returned error: %v", err)
	}

	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}

	if sessions[0].ID != "ses_two" {
		t.Fatalf("expected latest session first, got %q", sessions[0].ID)
	}

	filtered, err := adapter.ListSessions(projectOne, 10)
	if err != nil {
		t.Fatalf("ListSessions with project filter returned error: %v", err)
	}

	if len(filtered) != 1 {
		t.Fatalf("expected 1 filtered session, got %d", len(filtered))
	}

	if filtered[0].ID != "ses_one" {
		t.Fatalf("expected filtered session id ses_one, got %q", filtered[0].ID)
	}

	if filtered[0].FirstMessage != "How do I fix this?" {
		t.Fatalf("unexpected first message: %q", filtered[0].FirstMessage)
	}

	if filtered[0].UserMessageCount != 1 {
		t.Fatalf("expected user message count 1, got %d", filtered[0].UserMessageCount)
	}

	messages, err := adapter.GetSession("ses_one", 0, 10)
	if err != nil {
		t.Fatalf("GetSession returned error: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	if messages[0].Role != "user" || messages[0].Content != "How do I fix this?" {
		t.Fatalf("unexpected first message: role=%q content=%q", messages[0].Role, messages[0].Content)
	}

	if messages[1].Role != "assistant" || messages[1].Content != "Use SQLite fallback." {
		t.Fatalf("unexpected assistant message: role=%q content=%q", messages[1].Role, messages[1].Content)
	}

	if messages[1].Metadata["model"] != "gpt-5.3-codex" {
		t.Fatalf("expected assistant model metadata, got %#v", messages[1].Metadata["model"])
	}

	results, err := adapter.SearchSessions(projectOne, "sqlite fallback", 10)
	if err != nil {
		t.Fatalf("SearchSessions returned error: %v", err)
	}

	if len(results) != 1 || results[0].ID != "ses_one" {
		t.Fatalf("expected one search hit for ses_one, got %#v", results)
	}
}
