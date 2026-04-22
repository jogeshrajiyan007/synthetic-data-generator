AXA GovernGPT
Platform Architecture & Technical Reference
Version 4.0.0  |  AXA Global Business Services  |  DAAS DMG Team
Author: Jogesh Rajiyan, Analyst II — Data Quality & Governance
Classification: Internal  |  Domain: Insurance
1. Executive Summary
AXA GovernGPT is an enterprise-grade AI-powered Data Governance platform built for AXA Insurance. It combines a Retrieval-Augmented Generation (RAG) engine for governance knowledge with a live agentic SQL execution layer against Databricks, and a professional document generation capability. The platform is designed to enable Data Governance teams, Data Engineers, and Business Analysts to interrogate complex insurance datasets, enforce data governance policies, and produce professional documentation — all through a single conversational interface.
GovernGPT is built on FastAPI (Python 3.11), Next.js 14, ChromaDB, LangChain 0.2.x, and GPT-4o via the AXA SecureGPT Hub (Azure OpenAI OAuth2). Embeddings are generated locally using the all-MiniLM-L6-v2 model — no data leaves the AXA network for embedding operations.
2. System Architecture Overview
GovernGPT is structured as a two-tier application: a Python backend exposing a REST API, and a React/Next.js frontend. They communicate exclusively over HTTP. All AI inference routes through the AXA SecureGPT Hub — no direct OpenAI API calls are made.
2.1 Technology Stack
2.2 Directory Structure
The project uses two separate root directories:
StealthProject-Sandbox/
backend/                  ← FastAPI application root
main.py                 ← App factory, lifespan, router registration
config.py               ← SecureGPT credentials, paths, governance taxonomy
auth.py                 ← Thread-safe OAuth2 TokenManager
llm.py                  ← SecureGPTChat wrapper, embeddings singleton
db.py                   ← SQLite utilities (WAL mode, auto-schema)
models/schemas.py       ← Pydantic request/response models
routers/                ← FastAPI route handlers
ingest.py             ←   POST /api/ingest
query.py              ←   POST /api/query/
databricks.py         ←   /api/databricks/*
sql_agent.py          ←   /api/sql-agent/*
document.py           ←   /api/document/*
services/
rag.py                ← Hybrid RAG + SQL Agent routing engine
ingestion.py          ← File loaders, CSV profiler
chunking.py           ← RecursiveTextSplitter + metadata enrichment
vectorstore.py        ← ChromaDB build/load/add
memory.py             ← SQLite conversation history
document_generator.py ← LLM → JSON → Node.js → .docx pipeline
databricks/
client.py           ←   AES-256 PAT store, SQL execution
catalog.py          ←   Hive Metastore + Unity Catalog browser
profiler.py         ←   Statistical profiling, PII detection
ingestion.py        ←   Profile → chunk → vectorise pipeline
approved_sources.py ←   Per-session SQL whitelist (SQLite)
chroma_db/              ← Persisted ChromaDB vector store
llm_logs.db             ← SQLite: history, logs, connections
generated_docs/         ← Output .docx files
models/all-MiniLM-L6-v2/ ← Local embedding model
frontend-governgpt/       ← Next.js 14 application root
src/
app/                  ← Next.js App Router
components/           ← React components
chat/               ←   ChatWindow, MessageBubble, PromptGuidePanel
databricks/         ←   DatabricksPanel, CatalogBrowser, ConnectModal
sources/            ←   SourcePanel, UploadDropzone, FileItem
session/            ←   SessionGrid, CreateSessionModal
lib/api.ts            ← Type-safe HTTP client
store/index.ts        ← Zustand store (persisted)
types/index.ts        ← Shared TypeScript interfaces
3. RAG Engine — Conversational Document Intelligence
The RAG (Retrieval-Augmented Generation) engine is the core knowledge layer of GovernGPT. It ingests structured and unstructured documents, builds a persistent vector store, and answers governance questions by retrieving semantically relevant context and passing it to GPT-4o. All conversation turns are stored in SQLite for multi-turn memory.
3.1 Document Ingestion Pipeline
The ingestion pipeline processes 12 file types through format-specific loaders:
3.2 Chunking and Metadata Enrichment
After loading, all documents pass through chunk_and_enrich() which uses LangChain's RecursiveCharacterTextSplitter (chunk_size=800, overlap=100, separators: \n\n, \n, '. ', ' ').
Each chunk receives a rich metadata envelope persisted to both ChromaDB and SQLite:
3.3 Vector Store (ChromaDB)
Chunks are embedded using all-MiniLM-L6-v2 (384-dimensional vectors, L2-normalised) and stored in a persistent ChromaDB collection named 'governance_docs'. All metadata values are flattened to strings before storage (ChromaDB rejects lists/booleans). The vector store persists on disk at chroma_db/ and survives server restarts.
The retriever uses Maximum Marginal Relevance (MMR) search:
fetch_k = k × 6 candidates retrieved by cosine similarity
k × 2 selected by MMR algorithm (diversity over pure relevance)
Multi-source guarantee: if only 1 source represented, force-fetches from all other source files
Excluded tables: chunks from removed Databricks ingest items are filtered before context is built
1,500 character truncation per chunk prevents context window overflow
3.4 Query Routing: 3-Stage Gate
Every query entering query_governance() passes through a 3-stage routing gate before any LLM call:
3.5 Standard RAG Path (NOT_SQL intent)
When a query is classified as NOT_SQL, the standard conversational RAG chain executes:
Step 1 — Condense: if chat_history is non-empty, GPT-4o rewrites the question into a self-contained standalone question incorporating conversation context
Step 2 — Retrieve: MMR retriever fetches k=5 chunks from ChromaDB using the condensed question
Step 3 — Format: chunks are formatted with [Source: filename] headers
Step 4 — Answer: QA_PROMPT passes context + history + question to GPT-4o
Step 5 — Persist: human and AI messages saved to conversation_history in SQLite
The QA_PROMPT embeds a full insurance-industry expert persona covering Data Architecture, Data Engineering, Modelling, Governance/Compliance (GDPR, Solvency II, IFRS 17, BCBS 239), AI/Analytics, and BI/Reporting. Response structure templates are provided for each question type.
3.6 Conversation Memory
4. SQL Agent — Agentic Live Data Analysis
The SQL Agent is a 6-step agentic loop that classifies user intent, generates SQL, executes it against live Databricks tables, interprets results, and surfaces governance signals. It is activated only when SQL Agent is toggled ON and approved tables exist in the session.
4.1 Databricks Connection Management
Connections are stored in SQLite with the PAT encrypted using AES-256 (Fernet):
User provides: connection name, workspace URL, HTTP path, PAT, default catalog
PAT is encrypted via encrypt_pat() before SQLite storage — never stored in plaintext
Decryption key derived from CLIENT_ID + CLIENT_SECRET + '_governgpt_key' via PBKDF2-HMAC-SHA256
Connection tested at save time via Databricks SQL Connector test query
Each connection assigned a UUID connection_id referenced in all subsequent calls
4.2 Table Ingestion into Knowledge Set
Before SQL queries can run, tables must be profiled and ingested:
4.3 The 6-Step Agentic Loop
Once intent is confirmed as SQL (Stage 3 gate passed), run_sql_agent() executes:
4.4 Security Architecture
5. Document Generation (.docx)
GovernGPT can produce professional Word documents (.docx) on demand. The pipeline uses a 3-step approach: LLM generates structured JSON, a Node.js script builds the document using the docx@9.6.1 library, and the file is served for download.
5.1 Trigger Detection
The frontend's DOC_TRIGGER regex detects documentation requests before sending to the API:
/(create|generate|write|produce|build|make|prepare|draft|design)
.{0,30}(documentation|document|report|data catalog|catalog|
summary report|write-up|briefing|specification|data dictionary|runbook)/i
Matched queries bypass the RAG/SQL path and call POST /api/document/generate directly.
5.2 Generation Pipeline
6. Frontend Architecture
6.1 Application Structure
The frontend is a Next.js 14 App Router application with no server-side data fetching — all data flows through the api.ts client to the FastAPI backend.
6.2 State Management (Zustand)
Global state is managed by a single Zustand store (store/index.ts) persisted to localStorage:
6.3 SQL Approval Flow
SQL approval follows a strict explicit-consent model:
Ingest via ↓ button in CatalogBrowser → table appears in INGESTED TABLES section
'Use SQL' toggle on TableIngestCard → calls handleToggleApprove → setApprovedTables() in store
Sync call to POST /api/sql-agent/approved-sources/sync keeps backend SQLite in sync
ChatWindow reads approvedTables from store; hasSqlSources = approvedTables.length > 0
SQL Agent toggle in settings disabled when hasSqlSources = false
When last table toggled off or removed: clearApprovedTables() removes entire session key from store
On mount: reconciliation effect removes stale approvals for non-existent ingest items
excludedTables computed as ingestedFullTables.filter(t => !approvedTables.includes(t)) and sent with every query
7. End-to-End Data Flow
7.1 File Ingestion Flow
User drags file onto UploadDropzone → POST /api/ingest with file + knowledge_set
Backend saves to loading_zone/, runs format-specific loader, builds Documents
chunk_and_enrich(): RecursiveTextSplitter (800/100) → metadata enrichment → SQLite persist
build_vector_store() or add_to_vector_store(): embed via all-MiniLM-L6-v2 → ChromaDB
Response: chunk count, row count, duration. File appears in Sources panel.
7.2 RAG Query Flow
User types question → ChatWindow sendMessage() → POST /api/query/
query.py passes request to query_governance() with excluded_tables from frontend
Stage 1: _keyword_intent() — instant regex classification
Stage 2: LLM intent classifier if ambiguous (INTENT_PROMPT → GPT-4o)
Stage 3: Gate — NOT_SQL raises StopIteration → falls to RAG path
RAG: condense question → MMR retrieve (k=5, filtered) → format docs → QA_PROMPT → GPT-4o
save_turn() persists to SQLite. Response includes sources_used, chunks_retrieved, tokens.
7.3 SQL Agent Query Flow
Stage 3 gate confirms SQL intent → run_sql_agent() called with intent_override
Step 2: THINK_ALOUD_PROMPT → GPT-4o → italic strip shown in chat
Step 3: SQL_GENERATION_PROMPT (stateless, intent-aware) → GPT-4o → single SQL statement
Pre-execution: DDL block + table whitelist check → OUT_OF_SCOPE if either fails
Step 4: DatabricksClient.execute_query() → list of row dicts (max 200)
Step 5: INTERPRETATION_PROMPT (stateless) → GPT-4o → 2-4 bullet findings
Step 6: GOVERNANCE_OVERLAY_PROMPT (if governance context present) → ⚠️ note
Response: sql_intent, sql_executed, sql_results[], sql_row_count, think_aloud, governance_note
7.4 Document Generation Flow
ChatWindow DOC_TRIGGER regex matches → api.document.generate() → POST /api/document/generate
Router: load full session history (get_full_history) + RAG context from ChromaDB
DOC_CONTENT_PROMPT → GPT-4o → structured JSON (title, sections[], bullets, tables)
Write gen_{uuid}.js to generated_docs/, execute with NODE_PATH=backend/node_modules
docx@9.6.1 builds AXA-branded .docx, Packer.toBuffer → write file
Cleanup temp JS. Return {file_name, download_url, title, section_count}
MessageBubble renders download card with 'Download .docx' button
8. API Reference
9. SQLite Database Schema
All relational state is stored in llm_logs.db (WAL journal mode, 30s busy timeout):
10. Built-in Governance & Compliance Framework
10.1 PII Detection
PII signals are scanned at ingest time and at query time:
Ingest-time: chunking.py scans for SSN, Aadhar, passport, DOB, bank account, credit card, policy number, claimant ID
Profiler-time: _detect_pii_type() checks column names in Databricks tables
Query-time: governance_overlay generates ⚠️ PII warnings when PII-containing chunks are retrieved
10.2 Sensitivity Classification
10.3 Insurance Regulatory Coverage
11. Sequential Prompting Framework
GovernGPT is optimised for a 3-phase analytical workflow. Mixing phases causes routing conflicts and degrades response quality. The ⓘ button in the chat header opens an interactive version of this guide.
12. Deployment & Configuration
12.1 Prerequisites
12.2 Key pip Packages
pip install fastapi uvicorn langchain langchain-openai langchain-community
langchain-core langchain-text-splitters langchain-huggingface
chromadb sentence-transformers databricks-sql-connector
cryptography pandas openpyxl pypdf unstructured python-docx
pillow pytesseract httpx
12.3 Starting the Application
Backend (from backend/ directory)
uvicorn main:app --reload --host 0.0.0.0 --port 8000
Frontend (from frontend-governgpt/ directory)
npm run dev        # development — http://localhost:3000
npm run build && npm start  # production
12.4 Environment Notes
config.py contains AXA SecureGPT credentials — do not commit to public repositories
chroma_db/ and llm_logs.db persist all data — back up before server migrations
generated_docs/ contains .docx files — clean periodically or implement TTL
The all-MiniLM-L6-v2 model must be present locally — embedding fails silently otherwise
SSL verification is disabled for AXA corporate proxy (verify=False in httpx) — intentional
GovernGPT v4.0.0  ·  AXA Global Business Services  ·  DAAS DMG  ·  Jogesh Rajiyan
| Core Capabilities
• Conversational RAG over uploaded documents and Databricks table profiles
• Agentic SQL execution with intent detection, source enforcement, and governance overlay
• Professional .docx document generation from session context
• SQLite-persisted conversation memory for multi-turn analysis sessions
• AES-256 encrypted Databricks PAT storage
• Insurance-domain expertise embedded in every LLM prompt (GDPR, Solvency II, IFRS 17) |
| --- |
Layer
Technology
Version
Purpose
LLM Inference
AXA SecureGPT Hub (Azure OpenAI)
GPT-4o-2024-05-13
All LLM generation, intent classification, interpretation
Embeddings
HuggingFace all-MiniLM-L6-v2
Local (CPU)
Document and query embedding — runs fully offline
Vector Store
ChromaDB
Persistent on disk
Semantic similarity search over document chunks
Backend Framework
FastAPI + Uvicorn
Python 3.11+
REST API, async request handling, CORS
LLM Orchestration
LangChain 0.2.x (LCEL)
>=0.2
Prompt chains, retriever, output parsers
Relational Store
SQLite (WAL mode)
Built-in
Conversation memory, logs, connections, approvals
Frontend
Next.js 14 (App Router)
14.x
React 18 SPA with server components
State Management
Zustand + localStorage
4.x
Session state, approved tables, ingest items
Document Generation
docx@9.6.1 (Node.js)
9.6.1
Professional .docx creation via JS script
Databricks Connector
databricks-sql-connector
Latest
Live SQL execution against Databricks SQL Warehouses
Authentication
OAuth2 Client Credentials
AXA OneLogin
Thread-safe token rotation for SecureGPT Hub
PAT Encryption
Python cryptography (Fernet)
AES-256
Databricks PATs encrypted at rest in SQLite
File Type
Loader
Special Handling
.pdf
PyPDFLoader (LangChain)
Page-level splitting, page number preserved in metadata
.docx / .doc
UnstructuredWordDocumentLoader
Structure-aware extraction preserving headings
.csv
Custom _profile_csv()
Full statistical profiling — NO raw rows stored (see below)
.xlsx / .xls
pandas read_excel()
Multi-sheet: each sheet becomes a separate Document
.json
Custom loader
Pretty-printed JSON as single document
.xml
UnstructuredXMLLoader
Tag-aware extraction
.md
UnstructuredMarkdownLoader
Markdown structure preserved
.txt
TextLoader (UTF-8)
Plain text, encoding-safe
.jpg / .jpeg / .png
Pillow + pytesseract
OCR extraction — text from images
| CSV Profiling: Statistical Representation (Not Raw Data)
CSV files are NOT stored as raw rows. Instead, _profile_csv() computes a full statistical
profile per column: dtype, null count/%, unique count, value distributions, mean/median/
mode/std/skew/kurtosis for numerics, date ranges for temporal columns, top-20 categoricals,
5-bucket histograms, strong correlations (|r| > 0.5), and a 5-row sample.
The LLM receives computed insights — not raw tabular data. This prevents context overflow
and keeps sensitive data off the LLM wire. |
| --- |
Metadata Field
Source
Purpose
doc_id
UUID4 generated
Unique chunk identifier
source_file
File path basename
Source attribution in responses
source_type
'file' or 'databricks'
Controls RAG filtering and exclusion logic
file_type
Extension
Schema context for SQL agent
sensitivity
Signal-based detection
Restricted / Confidential / Internal / Public
contains_pii
PII keyword scan
Triggers governance overlay warnings
department
Keyword scoring
Claims / Underwriting / Compliance / Finance / IT
governance_themes
Taxonomy match
GDPR / Solvency II / IFRS 17 / data quality etc.
keywords
TF-IDF top-8
Searchability and relevance scoring
owner
Configured default
DAAS DMG (configurable)
ingested_at
Timestamp
Temporal ordering for recent data preference
| S1 | Keyword Pre-Classifier (_keyword_intent)
Instant regex-based classification. Catches obvious SQL patterns (table name mentioned, strong data verbs: 'show me', 'fetch', 'give me N rows') and obvious NOT_SQL patterns (analytical openers: 'why', 'how does', 'can you build'). No LLM call. Returns in <1ms. |
| --- | --- |
| S2 | LLM Intent Classifier (INTENT_PROMPT → GPT-4o)
Only called if Stage 1 returns None (ambiguous). GPT-4o classifies into one of 8 intent types: SIMPLE_LOOKUP, AGGREGATION, COMPARISON, JOIN_OPERATION, MULTI_STEP, CREATE_OBJECT, OUT_OF_SCOPE, NOT_SQL. Receives the approved tables list as context so it can detect unapproved table references. |
| --- | --- |
| S3 | SQL Gate (Hard Block)
Only SIMPLE_LOOKUP, AGGREGATION, COMPARISON, JOIN_OPERATION, MULTI_STEP, CREATE_OBJECT proceed to run_sql_agent(). NOT_SQL falls through via StopIteration to standard RAG. This prevents governance questions from ever reaching the SQL agent. |
| --- | --- |
Property
Value
Storage
SQLite table: conversation_history
Identifier
(session_id, knowledge_set) — maps to frontend Knowledge Set project ID
Schema
id, session_id, knowledge_set, role, content, run_id, token_count, created_at
Window
DEFAULT_WINDOW_K = 6 turns (12 messages) for active context
Full history
get_full_history() returns all turns — used by document generator
Persistence
Survives server restarts. Cleared only by explicit user action.
Journal mode
WAL (Write-Ahead Logging) for concurrent read/write safety
| 01 | Statistical Profiling (profiler.py)
profile_table() connects via DatabricksClient and executes multiple analytical queries: row count, column-level null/unique/cardinality stats, numeric mean/std/min/max/percentiles, datetime ranges, top-N categorical distributions, duplicate estimation, PII detection on column names, sensitivity classification, STRUCT/ARRAY/MAP type parsing. |
| --- | --- |
| 02 | Document Conversion (profile_to_documents())
Profile data is converted into LangChain Documents: one schema overview doc, one per-column-stats doc, one sample rows doc. STRUCT columns get dedicated documents with nested field rendering. All docs tagged source_type='databricks' for filtering. |
| --- | --- |
| 03 | Chunk → Embed → Store
Documents pass through chunk_and_enrich() then into the ChromaDB vector store. Schema context is now available for RAG retrieval, providing the SQL agent with column names, data types, and distributions during SQL generation. |
| --- | --- |
| 04 | SQL Approval (Manual)
After ingestion, the table appears in INGESTED TABLES. User toggles 'Use SQL' on the TableIngestCard. This calls setApprovedTables() in Zustand store and syncs to the backend via POST /api/sql-agent/approved-sources/sync. No auto-approval on ingest. |
| --- | --- |
| STEP 1 | Intent Override
If called from rag.py with intent_override, uses the pre-classified intent directly — skips internal LLM classification. For standalone /api/sql-agent/query calls, runs keyword classifier then LLM fallback. |
| --- | --- |
| STEP 2 | Think Aloud (THINK_ALOUD_PROMPT)
GPT-4o generates a 2-3 sentence plain-English explanation of what SQL it is about to run: which table, which columns, what logic. This is displayed in the chat as the italic green strip before the main answer. |
| --- | --- |
| STEP 3 | SQL Generation (SQL_GENERATION_PROMPT)
Stateless: no chat_history passed. GPT-4o receives the approved tables list, detected intent, schema context from RAG, and the user question. Intent-specific examples are embedded in the prompt to enforce correct patterns: AGGREGATION → COUNT(*)/GROUP BY, DISTINCT → SELECT DISTINCT. Markdown fences stripped. Single statement enforced. |
| --- | --- |
| STEP 4 | Pre-Execution Validation
Two security checks before any SQL touches Databricks: (1) PROHIBITED_PATTERNS regex blocks DROP/DELETE/INSERT/UPDATE/TRUNCATE/ALTER/GRANT/REVOKE — returns OUT_OF_SCOPE immediately. (2) _validate_tables_in_sql() extracts all FROM/JOIN references and verifies each against the approved_tables whitelist. Unapproved tables return OUT_OF_SCOPE with the table name listed. |
| --- | --- |
| STEP 5 | Execution (DatabricksClient)
Multi-statement detection: if SQL contains multiple SELECTs, checks for 'all tables' keywords to run _execute_multi_table_query() (sequential SELECT * LIMIT N per approved table, capped at 5). Single statements execute via client.execute_query(limit=200). Errors translated to plain English via ERROR_TRANSLATION_PROMPT with available column names for context. |
| --- | --- |
| STEP 6 | Interpret + Governance Overlay
INTERPRETATION_PROMPT (stateless — no chat_history) receives question, SQL, row_count, and first 50 result rows as JSON (capped at 8,000 chars). GPT-4o produces 2-4 bullet findings. If governance_context exists (PII/sensitivity chunks from RAG), GOVERNANCE_OVERLAY_PROMPT produces a ⚠️ governance note with PII flags, data owner, sensitivity tier. |
| --- | --- |
Control
Implementation
Coverage
Table whitelist
approved_tables list passed per-request, overrides SQLite read
Prevents querying tables not explicitly approved
DDL/DML block
PROHIBITED_PATTERNS regex on generated SQL
Prevents any writes/deletes to Databricks
Table reference validation
_validate_tables_in_sql() with approved_tables
Catches SQL injection via table name manipulation
PAT encryption
Fernet AES-256, key from PBKDF2-HMAC
PATs never stored or transmitted in plaintext
Read-only enforcement
SQL Connector runs with warehouse-level read permissions
Defence in depth — even if validation bypassed
Result row cap
MAX_RESULT_ROWS = 200, LLM cap = 50 rows
Prevents data exfiltration via large dumps
| 01 | Full History Retrieval
routers/document.py calls get_full_history() — NOT the windowed load_history(). Every turn in the session is included. Messages are formatted as a User:/Assistant: transcript capped at 12,000 characters to respect context limits. RAG context fetched from ChromaDB. |
| --- | --- |
| 02 | Structured JSON Generation (DOC_CONTENT_PROMPT)
GPT-4o receives the full history transcript, RAG context (6,000 char cap), and user's document request. Returns pure JSON matching the document schema: title, subtitle, author, sections[] each with heading, level (1 or 2), and content[] items typed as paragraph, bullets, or table. Minimum 4 sections including Executive Summary and Recommendations. |
| --- | --- |
| 03 | Node.js .docx Assembly
A temporary gen_{uuid}.js script is written to generated_docs/ and executed via subprocess. NODE_PATH is set to both backend/node_modules and global npm prefix for module resolution. cwd is set to backend root. The script uses docx@9.6.1 to build an AXA-branded document: AXA blue (#00008F) headings, header with title + 'AXA GovernGPT', footer with page numbers, zebra-striped tables with AXA blue headers, US Letter format (12240×15840 DXA), 1" margins. |
| --- | --- |
| 04 | Serve + Cleanup
Output .docx saved to generated_docs/{title}_{10-char-uuid}.docx. Temp JS script deleted. Download URL /api/document/download/{filename} returned. Frontend renders AXA-branded download card in MessageBubble with title, section count, and Download .docx button. |
| --- | --- |
| Documentation Best Practice
For best results, explicitly specify what to include rather than 'document everything':
"Create a data governance report covering: (1) claims_casualty_bronze schema, (2) claim
status distribution from SQL analysis, (3) monthly filing trends, (4) GDPR compliance gaps."
This prevents the LLM from processing the full chat history blindly and reduces hallucination. |
| --- |
Component
File
Responsibility
Session Grid
session/SessionGrid.tsx
Landing page — create/open/delete Knowledge Set projects
Workspace
Workspace.tsx
Root layout: splits SourcePanel (left) and ChatWindow (right)
Source Panel
sources/SourcePanel.tsx
File upload tab + Databricks tab container
Upload Dropzone
sources/UploadDropzone.tsx
Drag-and-drop file ingestion, progress tracking
Databricks Panel
databricks/DatabricksPanel.tsx
Connection management, catalog browse, SQL approval
Catalog Browser
databricks/CatalogBrowser.tsx
Hierarchical catalog → schema → table tree (ingest-only, no auto-approve)
Table Ingest Card
databricks/TableIngestCard.tsx
Shows ingested table stats + Use SQL / SQL On toggle
Connect Modal
databricks/ConnectModal.tsx
New connection form (Field component defined at module level — prevents focus loss)
Chat Window
chat/ChatWindow.tsx
Message list, input, settings, SQL agent toggle, doc detection
Message Bubble
chat/MessageBubble.tsx
Renders: think-aloud, SQL results table, view SQL, governance note, source tags, doc download card
Prompt Guide Panel
chat/PromptGuidePanel.tsx
Sequential prompting guide side panel (ⓘ button)
State Slice
Type
Description
sessions
KnowledgeSet[]
All project sessions with metadata
files
Record<sessionId, SourceFile[]>
Uploaded file list and ingest status per session
chats
Record<sessionId, ChatMessage[]>
Full message history per session (in-memory only)
tokenUsage
Record<sessionId, TokenUsage>
Cumulative token counts for display
databricksConnections
DatabricksConnection[]
Saved connections (shared across sessions)
databricksIngestItems
Record<sessionId, DatabricksIngestItem[]>
Per-session ingested table cards with status
approvedTables
Record<sessionId, {connection_id, tables[]}>
SQL-approved tables per session (persisted to localStorage)
Method
Endpoint
Purpose
Key Parameters
POST
/api/ingest
Ingest uploaded files into knowledge set
file (multipart), knowledge_set
POST
/api/query/
Hybrid RAG + SQL Agent query
question, knowledge_set, session_id, enable_sql_agent, approved_tables, excluded_tables
GET
/api/query/history/{session_id}
Retrieve full conversation history
session_id, knowledge_set
DELETE
/api/query/history/{session_id}
Clear conversation memory
session_id, knowledge_set
POST
/api/databricks/connect
Save a new Databricks connection
connection_name, workspace_url, http_path, pat, catalog
GET
/api/databricks/{conn_id}/catalogs
List catalogs in connection
connection_id
GET
/api/databricks/{conn_id}/schemas/{catalog}
List schemas
connection_id, catalog
GET
/api/databricks/{conn_id}/tables/{catalog}/{schema}
List tables
connection_id, catalog, schema
POST
/api/databricks/{conn_id}/ingest
Profile + vectorise a Databricks table
connection_id, knowledge_set, catalog, db_schema, table
POST
/api/sql-agent/approved-sources/sync
Sync approved table list
session_id, knowledge_set, connection_id, tables[]
GET
/api/sql-agent/approved-sources
Get approved tables for session
session_id, knowledge_set
POST
/api/document/generate
Generate .docx from session context
question, knowledge_set, session_id
GET
/api/document/download/{filename}
Download generated .docx
filename
GET
/health
System health check
—
Table
Key Columns
Purpose
conversation_history
session_id, knowledge_set, role, content, run_id, token_count, created_at
Full conversation memory — all turns, all sessions
llm_responses
run_id, question, answer, mode, sources_used, sql_intent, sql_executed, sql_row_count, tokens
Audit log of all LLM responses
databricks_connections
connection_id, connection_name, workspace_url, encrypted_pat, http_path, catalog
Saved Databricks connections (PAT AES-256 encrypted)
approved_sources
session_id, knowledge_set, connection_id, full_table, catalog, db_schema, table_name
Per-session SQL-approved tables (backend mirror of Zustand store)
sql_agent_log
run_id, session_id, intent, sql, row_count, duration_seconds, has_error
SQL execution audit trail
document_chunks
doc_id, source_file, source_type, sensitivity, contains_pii, department, governance_themes, keywords
Metadata for every ingested chunk
Tier
Trigger Keywords
Default Behaviour
Restricted
confidential, restricted, secret, internal only
Highest governance overlay priority
Confidential
personal, private, sensitive, medical, claimant
PII masking suggestions surfaced
Internal
internal, proprietary, not for distribution
Standard governance note
Public
(no trigger matched)
Minimal governance overhead
Regulation
Coverage in GovernGPT
GDPR
PII detection, data subject rights context, retention policy Q&A, sensitivity flagging
Solvency II
Risk data quality requirements, pillar III reporting context, SCR/MCR data lineage Q&A
IFRS 17
Insurance contract data modelling, GMM/PAA measurement approach context
BCBS 239
Data aggregation principles, risk reporting accuracy, timeliness requirements
Phase
SQL State
Question Types
Example Prompts
01 — Dataset Exploration
USE SQL OFF
Schema, ownership, governance, DQ, compliance
What are these documents about? / What key fields are in this dataset? / What governance frameworks apply?
02 — SQL Analysis
USE SQL ON
Live data queries, distributions, aggregations, trends
Show me first 10 rows / How many claims by status? / What is the average resolution time?
03 — Documentation
Either
Document requests with explicit scope
Create a governance report covering schema, DQ findings, SQL results, and GDPR gaps.
| Anti-Patterns to Avoid
✗ Asking SQL data queries with USE SQL OFF → falls to RAG, returns documentation-style answers
✗ Asking governance questions with USE SQL ON → SQL agent may attempt to classify as SQL
✗ Asking 'document everything' → LLM processes full history blindly; specify what to include
✗ Removing a table from ingest without toggling SQL Off first → stale approval counts |
| --- |
Dependency
Version
Installation
Python
3.11+
python.org
Node.js
18+ (22.x tested)
nodejs.org
npm docx package
9.6.1
npm install docx (in backend/ directory)
pip packages
see below
pip install -r requirements.txt --break-system-packages
Local embedding model
all-MiniLM-L6-v2
Download to backend/models/all-MiniLM-L6-v2/
Databricks SQL Connector
Latest
pip install databricks-sql-connector