package adapters

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// OpencodeAdapter implements SessionAdapter for opencode CLI sessions.
// opencode stores sessions in ~/.local/share/opencode/opencode.db (SQLite).
// Legacy flat-file storage remains available as a fallback:
// ~/.local/share/opencode/storage/
// Structure:
// - project/[PROJECT_ID].json - project metadata (worktree path, vcs)
// - session/[PROJECT_ID]/ses_*.json - session metadata (title, timestamps)
// - message/ses_*/msg_*.json - individual messages in each session
type OpencodeAdapter struct {
	storageDir string
	dbPath     string
}

// NewOpencodeAdapter creates a new opencode session adapter.
func NewOpencodeAdapter() (*OpencodeAdapter, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to get home directory: %w", err)
	}

	baseDir := filepath.Join(homeDir, ".local", "share", "opencode")
	return &OpencodeAdapter{
		storageDir: filepath.Join(baseDir, "storage"),
		dbPath:     filepath.Join(baseDir, "opencode.db"),
	}, nil
}

// Name returns the adapter name.
func (o *OpencodeAdapter) Name() string {
	return "opencode"
}

func (o *OpencodeAdapter) openDB() (*sql.DB, error) {
	if _, err := os.Stat(o.dbPath); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite", o.dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open opencode database: %w", err)
	}

	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set sqlite busy_timeout: %w", err)
	}

	return db, nil
}

// opencodeProject represents a project file in storage/project/
type opencodeProject struct {
	ID       string `json:"id"`
	Worktree string `json:"worktree"`
	VCS      string `json:"vcs"`
	Time     struct {
		Created int64 `json:"created"`
	} `json:"time"`
}

// opencodeSession represents a session file in storage/session/[PROJECT_ID]/
type opencodeSession struct {
	ID        string `json:"id"`
	Version   string `json:"version"`
	ProjectID string `json:"projectID"`
	Directory string `json:"directory"`
	Title     string `json:"title"`
	Time      struct {
		Created int64 `json:"created"`
		Updated int64 `json:"updated"`
	} `json:"time"`
}

// opencodeMessage represents a message file in storage/message/[SESSION_ID]/
type opencodeMessage struct {
	ID        string                 `json:"id"`
	Role      string                 `json:"role"`
	System    interface{}            `json:"system,omitempty"` // Can be string or array
	Mode      string                 `json:"mode,omitempty"`
	Content   interface{}            `json:"content,omitempty"`
	Cost      float64                `json:"cost,omitempty"`
	Tokens    map[string]interface{} `json:"tokens,omitempty"`
	ModelID   string                 `json:"modelID,omitempty"`
	Time      map[string]interface{} `json:"time,omitempty"`
	SessionID string                 `json:"sessionID,omitempty"`
}

type opencodePartSummary struct {
	TextParts    []string
	PartTypes    map[string]int
	NonTextParts []map[string]interface{}
}

// ListSessions returns all opencode sessions for the given project.
// If projectPath is empty, returns sessions from ALL projects.
func (o *OpencodeAdapter) ListSessions(projectPath string, limit int) ([]Session, error) {
	sessions, err := o.listSessionsFromSQLite(projectPath, limit)
	if err == nil {
		return sessions, nil
	}

	fallbackSessions, fallbackErr := o.listSessionsFromFiles(projectPath, limit)
	if fallbackErr == nil {
		return fallbackSessions, nil
	}

	return nil, fmt.Errorf("failed to list opencode sessions via sqlite (%v) and file fallback (%w)", err, fallbackErr)
}

// listSessionsFromSQLite lists sessions from opencode.db.
func (o *OpencodeAdapter) listSessionsFromSQLite(projectPath string, limit int) ([]Session, error) {
	db, err := o.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return o.listSessionsFromSQLiteWithDB(db, projectPath, limit)
}

func (o *OpencodeAdapter) listSessionsFromSQLiteWithDB(db *sql.DB, projectPath string, limit int) ([]Session, error) {
	var absPath string
	if projectPath != "" {
		resolvedPath, err := filepath.Abs(projectPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path: %w", err)
		}
		absPath = resolvedPath
	}

	query := `
		SELECT s.id, s.title, s.time_created, p.worktree
		FROM session s
		JOIN project p ON p.id = s.project_id
	`
	args := make([]interface{}, 0, 2)

	if absPath != "" {
		query += " WHERE p.worktree = ?"
		args = append(args, absPath)
	}

	query += " ORDER BY s.time_created DESC"

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query sessions from sqlite: %w", err)
	}
	defer rows.Close()

	sessions := make([]Session, 0)
	for rows.Next() {
		var (
			sessionID string
			title     string
			createdAt int64
			worktree  string
		)

		if err := rows.Scan(&sessionID, &title, &createdAt, &worktree); err != nil {
			return nil, fmt.Errorf("failed to scan sqlite session row: %w", err)
		}

		firstMessage, userCount, firstErr := o.getFirstUserMessageAndCountFromSQLite(db, sessionID)
		if firstErr != nil {
			firstMessage = ""
			userCount = 0
		}

		sessions = append(sessions, Session{
			ID:               sessionID,
			Source:           "opencode",
			ProjectPath:      worktree,
			FirstMessage:     firstMessage,
			Summary:          title,
			Timestamp:        time.UnixMilli(createdAt),
			FilePath:         o.dbPath,
			UserMessageCount: userCount,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while iterating sqlite sessions: %w", err)
	}

	return sessions, nil
}

func (o *OpencodeAdapter) getFirstUserMessageAndCountFromSQLite(db *sql.DB, sessionID string) (string, int, error) {
	firstQuery := `
		SELECT json_extract(p.data, '$.text')
		FROM message m
		JOIN part p ON p.message_id = m.id
		WHERE m.session_id = ?
		  AND json_extract(m.data, '$.role') = 'user'
		  AND json_extract(p.data, '$.type') = 'text'
		ORDER BY m.time_created ASC, p.time_created ASC
		LIMIT 1
	`

	var firstText sql.NullString
	err := db.QueryRow(firstQuery, sessionID).Scan(&firstText)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", 0, fmt.Errorf("failed to query first user message: %w", err)
	}

	countQuery := `
		SELECT COUNT(DISTINCT m.id)
		FROM message m
		JOIN part p ON p.message_id = m.id
		WHERE m.session_id = ?
		  AND json_extract(m.data, '$.role') = 'user'
		  AND json_extract(p.data, '$.type') = 'text'
		  AND trim(COALESCE(json_extract(p.data, '$.text'), '')) <> ''
	`

	var userCount int
	if err := db.QueryRow(countQuery, sessionID).Scan(&userCount); err != nil {
		return "", 0, fmt.Errorf("failed to count user messages: %w", err)
	}

	firstMessage := ""
	if firstText.Valid {
		firstMessage = o.extractFirstLine(firstText.String)
	}

	return firstMessage, userCount, nil
}

// listSessionsFromFiles lists sessions from legacy flat-file storage.
func (o *OpencodeAdapter) listSessionsFromFiles(projectPath string, limit int) ([]Session, error) {
	storageDir := o.storageDir

	// Check if storage directory exists
	if _, err := os.Stat(storageDir); os.IsNotExist(err) {
		return []Session{}, nil
	}

	// If project path specified, find matching project ID
	var targetProjectID string
	if projectPath != "" {
		absPath, err := filepath.Abs(projectPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path: %w", err)
		}

		projectID, err := o.findProjectIDByPath(storageDir, absPath)
		if err != nil || projectID == "" {
			return []Session{}, nil // No matching project
		}
		targetProjectID = projectID
	}

	// List all sessions
	sessionDir := filepath.Join(storageDir, "session")
	projectDirs, err := os.ReadDir(sessionDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read session directory: %w", err)
	}

	var allSessions []Session
	for _, projectDir := range projectDirs {
		if !projectDir.IsDir() {
			continue
		}

		projectID := projectDir.Name()

		// Filter by project if specified
		if targetProjectID != "" && projectID != targetProjectID {
			continue
		}

		// Get project metadata for worktree path
		project, err := o.loadProject(storageDir, projectID)
		if err != nil {
			continue
		}

		// List sessions for this project
		sessions, err := o.listProjectSessions(storageDir, projectID, project.Worktree)
		if err != nil {
			continue
		}

		allSessions = append(allSessions, sessions...)
	}

	// Sort by timestamp (newest first)
	sort.Slice(allSessions, func(i, j int) bool {
		return allSessions[i].Timestamp.After(allSessions[j].Timestamp)
	})

	// Apply limit
	if limit > 0 && len(allSessions) > limit {
		allSessions = allSessions[:limit]
	}

	return allSessions, nil
}

// findProjectIDByPath finds a project ID by matching the worktree path
func (o *OpencodeAdapter) findProjectIDByPath(storageDir, targetPath string) (string, error) {
	projectDir := filepath.Join(storageDir, "project")
	files, err := filepath.Glob(filepath.Join(projectDir, "*.json"))
	if err != nil {
		return "", err
	}

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var project opencodeProject
		if err := json.Unmarshal(data, &project); err != nil {
			continue
		}

		if project.Worktree == targetPath {
			return project.ID, nil
		}
	}

	return "", nil
}

// loadProject loads project metadata
func (o *OpencodeAdapter) loadProject(storageDir, projectID string) (*opencodeProject, error) {
	projectFile := filepath.Join(storageDir, "project", projectID+".json")
	data, err := os.ReadFile(projectFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read project file: %w", err)
	}

	var project opencodeProject
	if err := json.Unmarshal(data, &project); err != nil {
		return nil, fmt.Errorf("failed to parse project JSON: %w", err)
	}

	return &project, nil
}

// listProjectSessions lists all sessions for a specific project
func (o *OpencodeAdapter) listProjectSessions(storageDir, projectID, worktree string) ([]Session, error) {
	sessionDir := filepath.Join(storageDir, "session", projectID)
	files, err := filepath.Glob(filepath.Join(sessionDir, "ses_*.json"))
	if err != nil {
		return nil, err
	}

	var sessions []Session
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var sess opencodeSession
		if err := json.Unmarshal(data, &sess); err != nil {
			continue
		}

		// Get first message content
		firstMessage, userCount, err := o.getFirstUserMessageAndCount(storageDir, sess.ID)
		if err != nil {
			firstMessage = "" // Continue even if we can't get first message
			userCount = 0
		}

		session := Session{
			ID:               sess.ID,
			Source:           "opencode",
			ProjectPath:      worktree,
			FirstMessage:     firstMessage,
			Summary:          sess.Title,
			Timestamp:        time.UnixMilli(sess.Time.Created),
			FilePath:         file,
			UserMessageCount: userCount,
		}

		sessions = append(sessions, session)
	}

	return sessions, nil
}

// getFirstUserMessageAndCount extracts the first user message from a session and counts all user messages.
func (o *OpencodeAdapter) getFirstUserMessageAndCount(storageDir, sessionID string) (string, int, error) {
	messageDir := filepath.Join(storageDir, "message", sessionID)
	files, err := filepath.Glob(filepath.Join(messageDir, "msg_*.json"))
	if err != nil {
		return "", 0, err
	}

	// Sort by filename (contains timestamp-like component)
	sort.Strings(files)

	firstMessage := ""
	userCount := 0

	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var msg opencodeMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		// Find first user message
		if msg.Role == "user" {
			content := o.extractMessageContent(msg.Content)
			if content != "" {
				userCount++
				if firstMessage == "" {
					firstMessage = o.extractFirstLine(content)
				}
			}
		}
	}

	return firstMessage, userCount, nil
}

// extractMessageContent converts message content to string.
func (o *OpencodeAdapter) extractMessageContent(content interface{}) string {
	summary := o.summarizeMessageContent(content)
	return strings.Join(summary.TextParts, "\n")
}

func (o *OpencodeAdapter) summarizeMessageContent(content interface{}) opencodePartSummary {
	summary := opencodePartSummary{
		TextParts:    make([]string, 0),
		PartTypes:    make(map[string]int),
		NonTextParts: make([]map[string]interface{}, 0),
	}

	switch v := content.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			summary.TextParts = append(summary.TextParts, v)
			summary.PartTypes["text"]++
		}
	case []interface{}:
		for _, item := range v {
			if part, ok := item.(map[string]interface{}); ok {
				o.addPartToSummary(&summary, part)
			}
		}
	case map[string]interface{}:
		o.addPartToSummary(&summary, v)
	}

	return summary
}

func (o *OpencodeAdapter) addPartToSummary(summary *opencodePartSummary, part map[string]interface{}) {
	partType := o.getPartType(part)
	summary.PartTypes[partType]++

	if partType == "text" {
		if text, ok := part["text"].(string); ok && strings.TrimSpace(text) != "" {
			summary.TextParts = append(summary.TextParts, text)
		}
		return
	}

	copyPart := make(map[string]interface{}, len(part))
	for k, v := range part {
		copyPart[k] = v
	}
	summary.NonTextParts = append(summary.NonTextParts, copyPart)
}

func (o *OpencodeAdapter) getPartType(part map[string]interface{}) string {
	if part == nil {
		return "unknown"
	}
	if partType, ok := part["type"].(string); ok && strings.TrimSpace(partType) != "" {
		return partType
	}
	if _, ok := part["text"]; ok {
		return "text"
	}
	return "unknown"
}

// extractFirstLine extracts the first non-empty line from text
func (o *OpencodeAdapter) extractFirstLine(text string) string {
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			if len(trimmed) > 200 {
				return trimmed[:200] + "..."
			}
			return trimmed
		}
	}
	return ""
}

// GetSession retrieves the full content of an opencode session with pagination.
func (o *OpencodeAdapter) GetSession(sessionID string, page, pageSize int) ([]Message, error) {
	messages, _, _, _, err := o.GetSessionPage(sessionID, page, pageSize, false)
	if err != nil {
		return nil, err
	}
	return messages, nil
}

// GetSessionPage retrieves one page of session messages plus pagination metadata.
// If fromEnd is true, page=0 means last page, page=1 means second-to-last, etc.
func (o *OpencodeAdapter) GetSessionPage(sessionID string, page, pageSize int, fromEnd bool) ([]Message, int, int, bool, error) {
	if page < 0 {
		page = 0
	}
	if pageSize <= 0 {
		pageSize = 20
	}

	messages, totalMessages, resolvedPage, hasMore, err := o.getSessionPageFromSQLite(sessionID, page, pageSize, fromEnd)
	if err == nil {
		return messages, totalMessages, resolvedPage, hasMore, nil
	}

	fallbackMessages, fallbackTotal, fallbackResolved, fallbackHasMore, fallbackErr := o.getSessionPageFromFiles(sessionID, page, pageSize, fromEnd)
	if fallbackErr == nil {
		return fallbackMessages, fallbackTotal, fallbackResolved, fallbackHasMore, nil
	}

	return nil, 0, page, false, fmt.Errorf("failed to get opencode session via sqlite (%v) and file fallback (%w)", err, fallbackErr)
}

func (o *OpencodeAdapter) getSessionPageFromSQLite(sessionID string, page, pageSize int, fromEnd bool) ([]Message, int, int, bool, error) {
	db, err := o.openDB()
	if err != nil {
		return nil, 0, page, false, err
	}
	defer db.Close()

	exists, err := o.sqliteSessionExists(db, sessionID)
	if err != nil {
		return nil, 0, page, false, err
	}
	if !exists {
		return nil, 0, page, false, fmt.Errorf("session not found: %s", sessionID)
	}

	totalMessages, err := o.countSessionMessagesFromSQLite(db, sessionID)
	if err != nil {
		return nil, 0, page, false, err
	}

	resolvedPage := resolvePage(page, pageSize, totalMessages, fromEnd)
	if resolvedPage < 0 {
		return []Message{}, totalMessages, resolvedPage, false, nil
	}

	offset := resolvedPage * pageSize
	if offset >= totalMessages {
		return []Message{}, totalMessages, resolvedPage, false, nil
	}

	rows, err := db.Query(`
		SELECT id, time_created, data
		FROM message
		WHERE session_id = ?
		ORDER BY time_created ASC, id ASC
		LIMIT ? OFFSET ?
	`, sessionID, pageSize, offset)
	if err != nil {
		return nil, 0, page, false, fmt.Errorf("failed to query sqlite message page: %w", err)
	}
	defer rows.Close()

	type messageRow struct {
		id        string
		createdAt int64
		raw       string
	}

	messageRows := make([]messageRow, 0, pageSize)
	messageIDs := make([]string, 0, pageSize)

	for rows.Next() {
		var row messageRow
		if err := rows.Scan(&row.id, &row.createdAt, &row.raw); err != nil {
			return nil, 0, page, false, fmt.Errorf("failed to scan sqlite message row: %w", err)
		}
		messageRows = append(messageRows, row)
		messageIDs = append(messageIDs, row.id)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, page, false, fmt.Errorf("failed while iterating sqlite message page: %w", err)
	}

	partsByMessageID, err := o.getMessagePartsByMessageID(db, messageIDs)
	if err != nil {
		return nil, 0, page, false, err
	}

	messages := make([]Message, 0, len(messageRows))
	for _, row := range messageRows {
		var msg opencodeMessage
		if err := json.Unmarshal([]byte(row.raw), &msg); err != nil {
			return nil, 0, page, false, fmt.Errorf("failed to parse sqlite message JSON: %w", err)
		}

		partSummary, ok := partsByMessageID[row.id]
		if !ok {
			partSummary = opencodePartSummary{PartTypes: map[string]int{}}
		}

		content := strings.Join(partSummary.TextParts, "\n")
		if content == "" {
			fallbackSummary := o.summarizeMessageContent(msg.Content)
			if len(partSummary.NonTextParts) == 0 && len(fallbackSummary.NonTextParts) > 0 {
				partSummary.NonTextParts = fallbackSummary.NonTextParts
			}
			if len(partSummary.PartTypes) == 0 && len(fallbackSummary.PartTypes) > 0 {
				partSummary.PartTypes = fallbackSummary.PartTypes
			}
			content = strings.Join(fallbackSummary.TextParts, "\n")
		}
		if partSummary.PartTypes == nil {
			partSummary.PartTypes = map[string]int{}
		}

		message := Message{
			Role:            msg.Role,
			Content:         content,
			Metadata:        make(map[string]interface{}),
			HasNonTextParts: len(partSummary.NonTextParts) > 0,
			PartTypes:       partSummary.PartTypes,
		}
		if len(partSummary.NonTextParts) > 0 {
			message.NonTextParts = partSummary.NonTextParts
		}

		message.Timestamp = time.UnixMilli(row.createdAt)
		if msg.Time != nil {
			if created := o.extractMessageCreatedAt(msg.Time); created > 0 {
				message.Timestamp = time.UnixMilli(created)
			}
		}

		if msg.ModelID != "" {
			message.Metadata["model"] = msg.ModelID
		}
		if msg.Mode != "" {
			message.Metadata["mode"] = msg.Mode
		}
		if msg.Cost > 0 {
			message.Metadata["cost"] = msg.Cost
		}
		if msg.Tokens != nil {
			message.Metadata["tokens"] = msg.Tokens
		}

		messages = append(messages, message)
	}

	hasMore := offset+len(messages) < totalMessages
	return messages, totalMessages, resolvedPage, hasMore, nil
}

func (o *OpencodeAdapter) sqliteSessionExists(db *sql.DB, sessionID string) (bool, error) {
	var exists int
	err := db.QueryRow("SELECT 1 FROM session WHERE id = ? LIMIT 1", sessionID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check sqlite session existence: %w", err)
	}
	return true, nil
}

func (o *OpencodeAdapter) countSessionMessagesFromSQLite(db *sql.DB, sessionID string) (int, error) {
	var total int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM message
		WHERE session_id = ?
	`, sessionID).Scan(&total); err != nil {
		return 0, fmt.Errorf("failed to count sqlite session messages: %w", err)
	}
	return total, nil
}

func (o *OpencodeAdapter) getMessagePartsByMessageID(db *sql.DB, messageIDs []string) (map[string]opencodePartSummary, error) {
	result := make(map[string]opencodePartSummary, len(messageIDs))
	if len(messageIDs) == 0 {
		return result, nil
	}

	const chunkSize = 400
	for start := 0; start < len(messageIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(messageIDs) {
			end = len(messageIDs)
		}

		chunk := messageIDs[start:end]
		placeholders := strings.Repeat("?,", len(chunk))
		placeholders = strings.TrimSuffix(placeholders, ",")

		query := fmt.Sprintf(`
			SELECT message_id, data
			FROM part
			WHERE message_id IN (%s)
			ORDER BY message_id ASC, time_created ASC
		`, placeholders)

		args := make([]interface{}, 0, len(chunk))
		for _, id := range chunk {
			args = append(args, id)
		}

		rows, err := db.Query(query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to query sqlite parts for message chunk: %w", err)
		}

		for rows.Next() {
			var messageID string
			var rawPart string
			if err := rows.Scan(&messageID, &rawPart); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan sqlite part row: %w", err)
			}

			var part map[string]interface{}
			if err := json.Unmarshal([]byte(rawPart), &part); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to parse sqlite part JSON: %w", err)
			}

			summary, ok := result[messageID]
			if !ok {
				summary = opencodePartSummary{
					TextParts:    make([]string, 0),
					PartTypes:    make(map[string]int),
					NonTextParts: make([]map[string]interface{}, 0),
				}
			}

			o.addPartToSummary(&summary, part)
			result[messageID] = summary
		}

		for _, id := range chunk {
			if _, ok := result[id]; !ok {
				result[id] = opencodePartSummary{PartTypes: map[string]int{}}
			}
		}

		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed while iterating sqlite parts: %w", err)
		}

		rows.Close()
	}

	return result, nil
}

func resolvePage(page, pageSize, totalMessages int, fromEnd bool) int {
	if !fromEnd {
		return page
	}

	if totalMessages == 0 {
		return 0
	}

	lastPage := (totalMessages - 1) / pageSize
	resolvedPage := lastPage - page
	if resolvedPage < 0 {
		return -1
	}

	return resolvedPage
}

func (o *OpencodeAdapter) extractMessageCreatedAt(raw map[string]interface{}) int64 {
	created, ok := raw["created"]
	if !ok {
		return 0
	}

	switch v := created.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case int:
		return int64(v)
	default:
		return 0
	}
}

// getSessionPageFromFiles retrieves one page of an opencode session from legacy flat files.
func (o *OpencodeAdapter) getSessionPageFromFiles(sessionID string, page, pageSize int, fromEnd bool) ([]Message, int, int, bool, error) {
	storageDir := o.storageDir
	messageDir := filepath.Join(storageDir, "message", sessionID)

	// Check if message directory exists
	if _, err := os.Stat(messageDir); os.IsNotExist(err) {
		return nil, 0, page, false, fmt.Errorf("session not found: %s", sessionID)
	}

	// Read all messages
	messages, err := o.readAllMessages(messageDir)
	if err != nil {
		return nil, 0, page, false, err
	}
	totalMessages := len(messages)

	resolvedPage := resolvePage(page, pageSize, totalMessages, fromEnd)
	if resolvedPage < 0 {
		return []Message{}, totalMessages, resolvedPage, false, nil
	}

	// Apply pagination
	start := resolvedPage * pageSize
	if start >= len(messages) {
		return []Message{}, totalMessages, resolvedPage, false, nil
	}

	end := start + pageSize
	if end > len(messages) {
		end = len(messages)
	}

	hasMore := end < totalMessages

	return messages[start:end], totalMessages, resolvedPage, hasMore, nil
}

// readAllMessages reads all messages from a session directory
func (o *OpencodeAdapter) readAllMessages(messageDir string) ([]Message, error) {
	files, err := filepath.Glob(filepath.Join(messageDir, "msg_*.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to list message files: %w", err)
	}

	// Sort by filename (contains timestamp)
	sort.Strings(files)

	var messages []Message
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var msg opencodeMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		summary := o.summarizeMessageContent(msg.Content)

		message := Message{
			Role:            msg.Role,
			Content:         strings.Join(summary.TextParts, "\n"),
			Metadata:        make(map[string]interface{}),
			HasNonTextParts: len(summary.NonTextParts) > 0,
			PartTypes:       summary.PartTypes,
		}
		if message.PartTypes == nil {
			message.PartTypes = map[string]int{}
		}
		if len(summary.NonTextParts) > 0 {
			message.NonTextParts = summary.NonTextParts
		}

		// Parse timestamp from time.created
		if msg.Time != nil {
			if created, ok := msg.Time["created"].(float64); ok {
				message.Timestamp = time.UnixMilli(int64(created))
			}
		}

		// Add metadata
		if msg.ModelID != "" {
			message.Metadata["model"] = msg.ModelID
		}
		if msg.Mode != "" {
			message.Metadata["mode"] = msg.Mode
		}
		if msg.Cost > 0 {
			message.Metadata["cost"] = msg.Cost
		}
		if msg.Tokens != nil {
			message.Metadata["tokens"] = msg.Tokens
		}

		messages = append(messages, message)
	}

	return messages, nil
}

// SearchSessions searches opencode sessions for the given query
func (o *OpencodeAdapter) SearchSessions(projectPath, query string, limit int) ([]Session, error) {
	matches, err := o.searchSessionsFromSQLite(projectPath, query, limit)
	if err == nil {
		return matches, nil
	}

	fallbackMatches, fallbackErr := o.searchSessionsFromFiles(projectPath, query, limit)
	if fallbackErr == nil {
		return fallbackMatches, nil
	}

	return nil, fmt.Errorf("failed to search opencode sessions via sqlite (%v) and file fallback (%w)", err, fallbackErr)
}

func (o *OpencodeAdapter) searchSessionsFromSQLite(projectPath, query string, limit int) ([]Session, error) {
	db, err := o.openDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var absPath string
	if projectPath != "" {
		resolvedPath, err := filepath.Abs(projectPath)
		if err != nil {
			return nil, fmt.Errorf("failed to get absolute path: %w", err)
		}
		absPath = resolvedPath
	}

	lowerLikeQuery := "%" + strings.ToLower(query) + "%"
	sqlQuery := `
		SELECT DISTINCT s.id, s.title, s.time_created, p.worktree
		FROM session s
		JOIN project p ON p.id = s.project_id
		WHERE (
			LOWER(s.title) LIKE ?
			OR EXISTS (
				SELECT 1
				FROM message m
				JOIN part pt ON pt.message_id = m.id
				WHERE m.session_id = s.id
				  AND json_extract(pt.data, '$.type') = 'text'
				  AND LOWER(COALESCE(json_extract(pt.data, '$.text'), '')) LIKE ?
			)
		)
	`

	args := []interface{}{lowerLikeQuery, lowerLikeQuery}
	if absPath != "" {
		sqlQuery += " AND p.worktree = ?"
		args = append(args, absPath)
	}

	sqlQuery += " ORDER BY s.time_created DESC"
	if limit > 0 {
		sqlQuery += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := db.Query(sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to search sqlite sessions: %w", err)
	}
	defer rows.Close()

	matches := make([]Session, 0)
	for rows.Next() {
		var (
			sessionID string
			title     string
			createdAt int64
			worktree  string
		)

		if err := rows.Scan(&sessionID, &title, &createdAt, &worktree); err != nil {
			return nil, fmt.Errorf("failed to scan sqlite search result: %w", err)
		}

		firstMessage, userCount, firstErr := o.getFirstUserMessageAndCountFromSQLite(db, sessionID)
		if firstErr != nil {
			firstMessage = ""
			userCount = 0
		}

		matches = append(matches, Session{
			ID:               sessionID,
			Source:           "opencode",
			ProjectPath:      worktree,
			FirstMessage:     firstMessage,
			Summary:          title,
			Timestamp:        time.UnixMilli(createdAt),
			FilePath:         o.dbPath,
			UserMessageCount: userCount,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed while iterating sqlite search results: %w", err)
	}

	return matches, nil
}

func (o *OpencodeAdapter) searchSessionsFromFiles(projectPath, query string, limit int) ([]Session, error) {
	// First, list all sessions
	sessions, err := o.listSessionsFromFiles(projectPath, 0)
	if err != nil {
		return nil, err
	}

	query = strings.ToLower(query)
	var matches []Session

	// Search through each session
	for _, session := range sessions {
		// Check if query is in title or first message
		if strings.Contains(strings.ToLower(session.Summary), query) ||
			strings.Contains(strings.ToLower(session.FirstMessage), query) {
			matches = append(matches, session)
			continue
		}

		// Search through full session content
		storageDir := o.storageDir
		messageDir := filepath.Join(storageDir, "message", session.ID)
		messages, err := o.readAllMessages(messageDir)
		if err != nil {
			continue
		}

		for _, msg := range messages {
			if strings.Contains(strings.ToLower(msg.Content), query) {
				matches = append(matches, session)
				break
			}
		}

		// Apply limit if we've found enough
		if limit > 0 && len(matches) >= limit {
			break
		}
	}

	return matches, nil
}
