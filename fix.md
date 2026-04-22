# GovernGPT
## Agentic Data Intelligence Platform Architecture & Technical Reference

**Version:** 4.0.0  
**Classification:** Technical Documentation  
**Domain:** Enterprise Data Governance & Intelligence

---

## 1. Executive Summary

GovernGPT is an enterprise-grade AI-powered Data Intelligence platform that combines a Retrieval-Augmented Generation (RAG) engine for governance knowledge with a live agentic SQL execution layer against Databricks, and professional document generation capabilities. The platform enables Data Governance teams, Data Engineers, and Business Analysts to interrogate complex datasets, enforce governance policies, and produce professional documentation — all through a single conversational interface.

GovernGPT is built on:
- **Backend:** FastAPI (Python 3.11)
- **Frontend:** Next.js 14 with React
- **Vector Store:** ChromaDB with all-MiniLM-L6-v2 embeddings
- **LLM Integration:** GPT-4o via OpenAI API
- **Data Processing:** LangChain 0.2.x
- **Database:** SQLite with WAL mode

All embeddings are generated locally using all-MiniLM-L6-v2 model — no data leaves your network for embedding operations.

---

## 2. System Architecture Overview

GovernGPT is structured as a two-tier application: a Python backend exposing a REST API, and a React/Next.js frontend. They communicate exclusively over HTTP. All AI inference is configurable to route through your preferred LLM provider.

### 2.1 Technology Stack

| Component | Technology | Version | Purpose |
| --- | --- | --- | --- |
| **Backend** | FastAPI | 0.104+ | REST API, async request handling |
| **Web Framework** | Python | 3.11+ | Core runtime |
| **Frontend** | Next.js | 14.0+ | React SSR, optimized builds |
| **Frontend State** | Zustand | 4.4+ | Client state management |
| **Vector Database** | ChromaDB | 0.4+ | Persistent vector storage |
| **LLM Framework** | LangChain | 0.2.x | Orchestration, chains, agents |
| **Embeddings** | all-MiniLM-L6-v2 | 1.0 | 384-dim local embeddings |
| **SQL Execution** | Databricks SDK | 0.14+ | Data lake query execution |
| **Document Gen** | python-docx | 0.8.11 | .docx file creation |
| **Local Database** | SQLite | 3.40+ | History, configs, connections |
| **Data Querying** | Databricks SQL | Latest | SQL warehouse backend |
| **Authentication** | OAuth2 / JWT | Industry standard | Secure API access |
| **Container Runtime** | Docker | Optional | Deployment isolation |

### 2.2 Directory Structure

```
GovernGPT/
├── backend/                    ← FastAPI application root
│   ├── main.py                ← App factory, lifespan, router registration
│   ├── config.py              ← LLM credentials, paths, governance taxonomy
│   ├── auth.py                ← Thread-safe OAuth2 TokenManager
│   ├── llm.py                 ← LLM chat wrapper, embeddings singleton
│   ├── db.py                  ← SQLite utilities (WAL mode, auto-schema)
│   ├── models/
│   │   └── schemas.py         ← Pydantic request/response models
│   ├── routers/               ← FastAPI route handlers
│   │   ├── ingest.py          ← POST /api/ingest
│   │   ├── query.py           ← POST /api/query/
│   │   ├── databricks.py      ← /api/databricks/*
│   │   ├── sql_agent.py       ← /api/sql-agent/*
│   │   └── document.py        ← /api/document/*
│   ├── services/
│   │   ├── rag.py             ← Hybrid RAG + SQL Agent routing engine
│   │   ├── ingestion.py       ← File loaders, CSV profiler
│   │   ├── chunking.py        ← RecursiveTextSplitter + metadata enrichment
│   │   ├── vectorstore.py     ← ChromaDB build/load/add
│   │   ├── memory.py          ← SQLite conversation history
│   │   ├── document_generator.py ← LLM → JSON → Node.js → .docx pipeline
│   │   └── databricks/
│   │       ├── client.py      ← AES-256 PAT store, SQL execution
│   │       ├── catalog.py     ← Hive Metastore + Unity Catalog browser
│   │       ├── profiler.py    ← Statistical profiling, PII detection
│   │       ├── ingestion.py   ← Profile → chunk → vectorise pipeline
│   │       └── approved_sources.py ← Per-session SQL whitelist (SQLite)
│   ├── chroma_db/             ← Persisted ChromaDB vector store
│   ├── llm_logs.db            ← SQLite: history, logs, connections
│   ├── generated_docs/        ← Output .docx files
│   └── models/all-MiniLM-L6-v2/ ← Local embedding model
│
├── frontend-governgpt/        ← Next.js 14 application root
│   ├── src/
│   │   ├── app/               ← Next.js App Router
│   │   ├── components/        ← React components
│   │   │   ├── chat/          ← ChatWindow, MessageBubble, PromptGuidePanel
│   │   │   ├── databricks/    ← DatabricksPanel, CatalogBrowser, ConnectModal
│   │   │   ├── sources/       ← SourcePanel, UploadDropzone, FileItem
│   │   │   └── session/       ← SessionGrid, CreateSessionModal
│   │   ├── lib/api.ts         ← Type-safe HTTP client
│   │   ├── store/index.ts     ← Zustand store (persisted)
│   │   └── types/index.ts     ← Shared TypeScript interfaces
│   ├── public/                ← Static assets
│   ├── next.config.js         ← Next.js configuration
│   └── package.json           ← Dependencies
│
└── docker-compose.yml         ← Local development setup
```

---

## 3. RAG Engine — Conversational Data Intelligence

The RAG (Retrieval-Augmented Generation) engine is the core knowledge layer of GovernGPT. It ingests structured and unstructured documents, builds a persistent vector store, and answers questions by retrieving semantically relevant context and passing it to the LLM. All conversation turns are stored in SQLite for multi-turn memory.

### 3.1 Document Ingestion Pipeline

The ingestion pipeline processes 12 file types through format-specific loaders:

| File Type | Loader | Key Features |
| --- | --- | --- |
| **.pdf** | PyPDF2 | Text extraction, page-level metadata |
| **.docx** | python-docx | Paragraph structure, table extraction |
| **.xlsx** | openpyxl | Sheet names, cell-level data |
| **.csv** | pandas | Auto-detect schema, statistical profile |
| **.txt** | Raw file read | UTF-8 encoding, line preservation |
| **.json** | json.loads() | Hierarchical key-value extraction |
| **.md** | Markdown parser | Section-level hierarchy |
| **.pptx** | python-pptx | Slide text, speaker notes |
| **.html** | BeautifulSoup | DOM structure, semantic tags |
| **.xml** | xml.etree | Tree traversal, attribute extraction |
| **.sql** | Raw read | Query parsing, comment removal |
| **.log** | Log parser | Timestamp, level, message extraction |

Each document receives:
- `source_id`: Unique identifier (filename + timestamp)
- `source_type`: File extension category
- `uploaded_by`: User who uploaded
- `uploaded_at`: ISO timestamp
- `file_size`: Bytes
- `page_count`: For multipage documents
- `extraction_method`: Loader used

### 3.2 Chunking and Metadata Enrichment

After loading, all documents pass through `chunk_and_enrich()` which uses LangChain's RecursiveCharacterTextSplitter with:
- **chunk_size:** 800 characters
- **overlap:** 100 characters
- **separators:** `['\n\n', '\n', '. ', ' ']` (semantic boundaries)

Each chunk receives a rich metadata envelope persisted to both ChromaDB and SQLite:

```python
metadata = {
    "source_id": "claims_policy_v2.pdf_2025-04-21",
    "source_type": "pdf",
    "chunk_index": 42,
    "total_chunks": 156,
    "page_number": 3,
    "section_title": "Data Quality Dimensions",
    "semantic_category": "governance",
    "pii_detected": False,
    "pii_types": [],
    "sensitivity_tier": "internal",
    "keywords": ["quality", "validation", "rules"],
    "char_start": 2450,
    "char_end": 3250,
    "chunk_hash": "sha256:abc123..."
}
```

### 3.3 Vector Store (ChromaDB)

Chunks are embedded using all-MiniLM-L6-v2 (384-dimensional vectors, L2-normalised) and stored in a persistent ChromaDB collection named `governance_docs`. All metadata values are flattened to strings before storage (ChromaDB rejects lists/booleans). The vector store persists on disk at `chroma_db/` and survives server restarts.

The retriever uses **Maximum Marginal Relevance (MMR)** search:
- `fetch_k = k × 6` candidates retrieved by cosine similarity
- `k × 2` selected by MMR algorithm (diversity over pure relevance)
- **Multi-source guarantee:** If only 1 source represented, force-fetch from all other source files
- **Excluded tables:** Chunks from removed ingest items are filtered before context building
- **Truncation:** 1,500 character truncation per chunk prevents context window overflow

### 3.4 Query Routing: 3-Stage Gate

Every query entering `query_governance()` passes through a 3-stage routing gate before any LLM call:

**Stage 1 — Regex Intent Detection**
```
Pattern matching for SQL keywords:
  ✓ SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP
  ✓ EXPLAIN, DESC, SHOW, CALL, EXECUTE
  ✗ Fails → proceed to Stage 2
```

**Stage 2 — LLM Intent Classifier**
```
Prompt: "Is this a request for SQL execution or general Q&A?"
Classification: SQL | RAG | MIXED
Confidence: 0.0–1.0
  ✓ SQL confidence > 0.7 → proceed to Stage 3
  ✗ Otherwise → route to standard RAG
```

**Stage 3 — Hard Gate (Approved Tables)**
```
If intent = SQL:
  ✓ Session has approved tables → run SQL Agent
  ✗ No approved tables → refuse SQL, suggest RAG
```

### 3.5 Standard RAG Path (NOT_SQL intent)

When a query is classified as NOT_SQL, the standard conversational RAG chain executes:

1. **Condense:** If chat_history is non-empty, GPT-4o rewrites the question into a self-contained standalone question incorporating conversation context
2. **Retrieve:** MMR retriever fetches k=5 chunks from ChromaDB using the condensed question
3. **Format:** Chunks formatted with `[Source: filename]` headers
4. **Answer:** QA_PROMPT passes context + history + question to LLM
5. **Persist:** Human and AI messages saved to `conversation_history` in SQLite

The QA_PROMPT embeds a full data governance expert persona covering:
- Data Architecture & Engineering
- Data Modelling & Semantics
- Governance & Compliance (GDPR, data residency, audit)
- Analytics & Business Intelligence
- AI/ML Governance

Response structure templates are provided for each question type.

### 3.6 Conversation Memory

All conversations are persisted in SQLite:

```sql
CREATE TABLE conversation_history (
    id INTEGER PRIMARY KEY,
    session_id TEXT,
    user_id TEXT,
    turn_number INTEGER,
    human_message TEXT,
    ai_response TEXT,
    sources TEXT,  -- JSON list of [source_id, chunk_index]
    intent TEXT,   -- SQL | RAG | MIXED
    created_at TIMESTAMP
);
```

Multi-turn context is retrieved as:
```
[Message 1] → [Message 2] → [Message 3] → [New Query]
```

---

## 4. SQL Agent — Agentic Live Data Analysis

The SQL Agent is a 6-step agentic loop that classifies user intent, generates SQL, executes it against live Databricks tables, interprets results, and surfaces governance signals. It is activated only when SQL Agent is toggled ON and approved tables exist in the session.

### 4.1 Data Connection Management

Connections are stored in SQLite with the authentication token encrypted using AES-256 (Fernet):

- User provides: connection name, workspace URL, HTTP path, PAT, default catalog
- Token is encrypted via `encrypt_pat()` before SQLite storage — never stored in plaintext
- Decryption key derived from environment secrets + PBKDF2-HMAC-SHA256
- Connection tested at save time via data warehouse test query
- Each connection assigned a UUID `connection_id` referenced in all subsequent calls

```python
{
    "connection_id": "uuid-1234-5678",
    "connection_name": "Production Warehouse",
    "workspace_url": "https://adb-xxxxx.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abc123",
    "encrypted_pat": "gAAAAAB...",  # AES-256 ciphertext
    "default_catalog": "main",
    "created_at": "2025-04-21T10:30:00Z",
    "last_tested": "2025-04-21T15:45:00Z",
    "test_status": "success"
}
```

### 4.2 Table Ingestion into Knowledge Set

Before SQL queries can run, tables must be profiled and ingested:

1. **Catalog Browse:** List available catalogs, schemas, tables
2. **Table Selection:** User selects table(s) for analysis
3. **Statistical Profile:** Compute row count, null %, unique %, data types
4. **Schema Extraction:** Column names, types, nullable flags, comments
5. **Sample Data:** Retrieve first 100 rows
6. **Metadata Chunk:** Create special chunk containing table schema + stats
7. **Index in ChromaDB:** Schema chunk added to vector store for semantic search
8. **SQLite Whitelist:** Approved table names stored in session-specific whitelist

### 4.3 The 6-Step Agentic Loop

Once intent is confirmed as SQL (Stage 3 gate passed), `run_sql_agent()` executes:

**Step 1 — Intent Override (User Explicit)**
```
Check if user provided:
  - Explicit catalog, schema, table names
  - Explicit column names
  - Explicit WHERE clauses

If yes → prepopulate generator context (reduces hallucination)
```

**Step 2 — Think Aloud**
```
Prompt LLM:
  "The user is asking: [question]
   Available tables: [whitelist]
   How would you approach this in SQL?
   Think step-by-step before writing SQL."

Output: Chain-of-thought reasoning (shown to user)
```

**Step 3 — SQL Generation**
```
Prompt LLM:
  "Given the reasoning above, write a SQL query.
   Constraint: Only SELECT queries.
   Constraint: Only approved tables.
   Constraint: LIMIT 200 rows.
   
   Query:"

Output: Raw SQL text
```

**Step 4 — Pre-Execution Validation**
```
1. Parse SQL with sqlparse library
2. Check statement type (only SELECT allowed)
3. Check for DDL/DML keywords (DENY: CREATE, DROP, INSERT, UPDATE, DELETE)
4. Extract table names, compare with whitelist
5. Extract column names, validate against schema
6. Check for suspicious patterns:
   - Recursive CTEs
   - Union with unvetted tables
   - Subqueries on non-whitelisted tables
   
If validation fails → refuse execution, suggest correction
```

**Step 5 — Execute Against Data Warehouse**
```
Execute via Databricks SQL Connector:
  - Query: Validated SQL
  - Warehouse: From connection config
  - Timeout: 60 seconds
  - Max rows: 200
  
Return:
  {
    "status": "success",
    "row_count": 42,
    "columns": ["col1", "col2"],
    "rows": [[...], [...], ...],
    "execution_time_ms": 450
  }
```

**Step 6 — Interpret + Governance Overlay**
```
Prompt LLM:
  "Query executed successfully. Results:
   [rows displayed]
   
   In 2-3 sentences, what do these results tell us?
   Are there any data governance signals
   (completeness, timeliness, accuracy)?
   
   Interpretation:"

Output: Natural language summary + governance flags
```

### 4.4 Security Architecture

**SQL Injection Prevention:**
- Parameterized queries only (never string concatenation)
- SQL parsing + AST validation before execution
- Whitelist enforcement at parse time

**Table Access Control:**
- Session-specific approved table list (SQLite whitelist)
- Users cannot query unapproved tables (hard gate at Step 4)
- Whitelist modified only via UI approval workflow

**PII/Sensitive Data Protection:**
- Automatic PII detection on column names (regex + ML)
- Flagged columns suppressed from results (or hashed)
- Audit log: who queried which tables when

**Encryption:**
- Connection PATs encrypted at rest (AES-256)
- Connection PATs decrypted in memory only when executing
- PAT never logged or exposed in error messages

---

## 5. Document Generation (.docx)

GovernGPT can produce professional Word documents (.docx) on demand. The pipeline uses a 3-step approach: LLM generates structured JSON, a Node.js script builds the document using the docx library, and the file is served for download.

### 5.1 Trigger Detection

The frontend's DOC_TRIGGER regex detects documentation requests before sending to the API:

```
/(create|generate|write|produce|build|make|prepare|draft|design)
\s+(a\s+)?(document|report|summary|analysis|brief|proposal|specification|memo|whitepaper)
/i
```

**Example triggers:**
- ✓ "Generate a data governance report"
- ✓ "Write a compliance summary"
- ✓ "Create a technical specification"
- ✗ "Show me the data" (no action)

### 5.2 Document Content Prompt

When triggered, the LLM receives:

```
You are a professional data governance consultant.
Based on the conversation so far:
  - User question: [query]
  - Retrieved documents: [context]
  - Query results: [data]

Generate a professional document covering:
  1. Executive Summary (2-3 sentences)
  2. Key Findings (bullet points)
  3. Governance Implications
  4. Recommendations
  5. Technical Details (if applicable)

Format your response as JSON:
{
  "title": "Document Title",
  "sections": [
    {"heading": "Executive Summary", "content": "..."},
    ...
  ]
}
```

### 5.3 Document Generation Pipeline

**Backend:** FastAPI receives JSON from LLM, triggers Node.js subprocess:

```python
# backend/services/document_generator.py

async def generate_document(doc_json: dict) -> str:
    """
    Generate .docx from JSON specification
    """
    # Write JSON to temp file
    json_path = f"/tmp/doc_{uuid4()}.json"
    with open(json_path, 'w') as f:
        json.dump(doc_json, f)
    
    # Execute Node.js script
    script_path = "backend/scripts/generate_docx.js"
    output_path = f"generated_docs/{uuid4()}.docx"
    
    process = await asyncio.create_subprocess_exec(
        "node", script_path, json_path, output_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    await process.wait()
    return output_path
```

**Frontend:** Node.js script uses docx library:

```javascript
// backend/scripts/generate_docx.js

const { Document, Packer, Paragraph, HeadingLevel, TextRun } = require('docx');
const fs = require('fs');

async function generateDocument(jsonPath, outputPath) {
    const docSpec = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
    
    const doc = new Document({
        sections: [{
            children: docSpec.sections.map(section => [
                new Paragraph({
                    text: section.heading,
                    heading: HeadingLevel.HEADING_1,
                    bold: true
                }),
                new Paragraph(section.content)
            ]).flat()
        }]
    });
    
    const buffer = await Packer.toBuffer(doc);
    fs.writeFileSync(outputPath, buffer);
    console.log(`Document saved: ${outputPath}`);
}

generateDocument(process.argv[2], process.argv[3]);
```

### 5.4 Document Serving

FastAPI serves the generated document:

```python
@router.get("/api/document/{doc_id}")
async def download_document(doc_id: str):
    """Download generated document"""
    path = f"generated_docs/{doc_id}.docx"
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        filename=f"governgpt_export_{doc_id}.docx"
    )
```

---

## 6. Frontend Architecture

### 6.1 Session Management

Sessions represent isolated conversation contexts. Each session:
- Has a unique `session_id` (UUID)
- Stores conversation history in SQLite
- Maintains approved Databricks tables (whitelist)
- Persists in Zustand store (localStorage)

```typescript
interface Session {
  session_id: string;
  created_at: Date;
  last_active: Date;
  title: string;
  conversation: Message[];
  approved_tables: {
    connection_id: string;
    table_names: string[];
  }[];
}

const useSessionStore = create((set) => ({
  sessions: [],
  createSession: () => { /* ... */ },
  deleteSession: (id) => { /* ... */ },
  updateSession: (id, data) => { /* ... */ }
}));
```

### 6.2 Chat Interface

**ChatWindow Component:**
- Displays conversation turns (human messages on right, AI on left)
- Renders source citations: `[Source: filename, page 3]`
- Shows thinking time for SQL queries
- Displays execution results inline

**PromptGuidePanel:**
- Suggests common queries based on context
- Shows available tables, columns
- Provides query templates

### 6.3 Data Catalog Explorer

**CatalogBrowser Component:**
- Browse: Catalog → Schema → Table
- View table schema (columns, types)
- See row count, null percentages
- Preview first 10 rows
- Add table to session whitelist

---

## 7. API Reference

### 7.1 Chat Query Endpoint

```
POST /api/query/

Request:
{
  "session_id": "uuid-1234",
  "message": "What tables contain claims data?",
  "use_sql_agent": false,
  "chat_history": []
}

Response:
{
  "session_id": "uuid-1234",
  "response": "Based on the governance documents...",
  "sources": [
    {
      "source_id": "data_catalog.pdf_2025-04-21",
      "chunk_index": 12,
      "relevance": 0.92
    }
  ],
  "intent": "RAG",
  "execution_time_ms": 2340
}
```

### 7.2 SQL Agent Endpoint

```
POST /api/sql-agent/execute

Request:
{
  "session_id": "uuid-1234",
  "connection_id": "uuid-5678",
  "question": "How many records in claims_raw?",
  "use_approved_tables": true
}

Response:
{
  "thinking": "User wants row count. I'll SELECT COUNT(*) FROM claims_raw.",
  "sql": "SELECT COUNT(*) as record_count FROM main.bronze.claims_raw LIMIT 200;",
  "results": {
    "status": "success",
    "row_count": 1,
    "columns": ["record_count"],
    "rows": [[5432100]],
    "execution_time_ms": 450
  },
  "interpretation": "The claims_raw table contains 5.4M records as of last refresh.",
  "governance_flags": []
}
```

### 7.3 Document Generation Endpoint

```
POST /api/document/generate

Request:
{
  "session_id": "uuid-1234",
  "query": "Generate a data governance report",
  "document_type": "report"
}

Response:
{
  "document_id": "doc-uuid-9999",
  "title": "Data Governance Report",
  "status": "success",
  "download_url": "/api/document/doc-uuid-9999",
  "generated_at": "2025-04-21T16:00:00Z"
}
```

---

## 8. Deployment Architecture

### 8.1 Local Development

```bash
# Backend
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload

# Frontend (separate terminal)
cd frontend-governgpt
npm install
npm run dev

# Both accessible at:
# Frontend: http://localhost:3000
# Backend API: http://localhost:8000
```

### 8.2 Production Deployment (App Service)

```yaml
# azure-pipelines.yml
stages:
  - Build (test, lint, build)
  - Deploy (push to App Service)
  - Monitor (Application Insights)

Environment Variables:
  - OPENAI_API_KEY
  - DATABASE_URL
  - ENCRYPTION_MASTER_KEY
```

---

## 9. Security Considerations

| Threat | Mitigation |
| --- | --- |
| **SQL Injection** | Parameterized queries, AST validation, whitelist enforcement |
| **Prompt Injection** | Input sanitization, guardrails on LLM prompts |
| **Token Exposure** | Encrypted at rest (AES-256), in-memory decryption only |
| **Data Exfiltration** | PII detection, output filtering, audit logging |
| **Unauthorized Access** | Session-based access control, approved table whitelist |
| **LLM Jailbreak** | System prompts constrain behavior, refuse unsafe requests |

---

## 10. Performance Characteristics

| Operation | Baseline | Optimized |
| --- | --- | --- |
| **Document Ingest** | 5 sec (10 MB file) | 2 sec (vectorization parallelized) |
| **RAG Query** | 3.2 sec | 1.8 sec (MMR caching) |
| **SQL Execution** | 2-5 sec (query dependent) | Same (warehouse-bound) |
| **Document Gen** | 8 sec | 4 sec (Node.js subprocess) |
| **Vector Search** | 150 ms (5 docs) | 95 ms (ChromaDB index) |

---

## 11. Roadmap & Future Enhancements

**v4.1 (Next Release)**
- [ ] Multi-LLM support (Claude, Gemini, local models)
- [ ] Streaming responses
- [ ] Advanced analytics (time-series forecasting)

**v5.0 (Major Release)**
- [ ] Knowledge graph construction
- [ ] Multi-agent orchestration
- [ ] Real-time data monitoring
- [ ] Collaborative features (shared sessions)

---

## Conclusion

GovernGPT combines the intelligence of RAG with the power of agentic SQL execution to create a truly conversational data intelligence platform. By grounding LLM responses in both governance documents and live data, it enables organizations to make data-driven decisions with confidence, speed, and transparency.

All components are designed for enterprise production use: secure, auditable, scalable, and maintainable.

---

**Document Version:** 4.0.0  
**Last Updated:** April 2025  
**Maintained By:** Data Platform Team
