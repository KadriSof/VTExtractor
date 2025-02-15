## Structure <br>
real_estate_pipeline/ <br>
├── .github/                     # GitHub Actions workflows for CI/CD <br>
│   └── workflows/ <br>
│       ├── ci.yml               # Continuous Integration workflow <br>
│       └── cd.yml               # Continuous Deployment workflow <br>
├── .vscode/                     # VSCode settings (optional, if using VSCode) <br>
├── config/                      # Configuration files <br>
│   ├── dev.env                  # Development environment variables <br>
│   ├── prod.env                 # Production environment variables <br>
│   └── logging.conf             # Logging configuration <br>
├── data/                        # Sample data for testing <br>
│   └── sample.pdf               # Example PDF file <br>
├── docs/                        # Documentation <br>
│   ├── architecture.md          # High-level architecture overview <br>
│   ├── api_endpoints.md         # API endpoint documentation <br>
│   └── setup_guide.md           # Setup and installation guide <br>
├── src/                         # Main application code <br>
│   ├── common/                  # Shared utilities and helpers <br>
│   │   ├── exceptions.py        # Custom exceptions <br>
│   │   ├── logger.py            # Logging setup <br>
│   │   └── utils.py             # Utility functions <br>
│   ├── ingestion/               # Ingestion service <br>
│   │   ├── service.py           # Main ingestion logic <br>
│   │   └── models.py            # Data models for ingestion <br>
│   ├── preprocessing/           # Preprocessing service <br>
│   │   ├── service.py           # Main preprocessing logic <br>
│   │   └── models.py            # Data models for preprocessing <br>
│   ├── extraction/              # Information extraction service <br>
│   │   ├── service.py           # Main extraction logic <br>
│   │   ├── models.py            # Data models for extraction <br>
│   │   └── prompts/             # LLM prompts for extraction <br>
│   │       └── property_prompts.txt <br>
│   ├── storage/                 # Storage service <br>
│   │   ├── service.py           # Main storage logic <br>
│   │   ├── models.py            # Data models for storage <br>
│   │   └── database/            # Database connection and operations <br>
│   │       ├── base.py          # Base database class <br>
│   │       ├── sql_db.py        # SQL database implementation <br>
│   │       └── cosmos_db.py     # Cosmos DB implementation (optional) <br>
│   ├── rag/                     # RAG system <br>
│   │   ├── service.py           # Main RAG logic <br>
│   │   ├── models.py            # Data models for RAG <br>
│   │   └── embeddings/          # Embedding generation and storage <br>
│   │       ├── openai_embeddings.py <br>
│   │       └── faiss_index.py   # FAISS index implementation <br>
│   ├── api/                     # FastAPI application <br>
│   │   ├── main.py              # FastAPI entry point <br>
│   │   ├── routers/             # API routers <br>
│   │   │   ├── ingestion.py     # Ingestion endpoints <br>
│   │   │   ├── extraction.py    # Extraction endpoints <br>
│   │   │   └── rag.py           # RAG endpoints <br>
│   │   └── schemas.py           # Pydantic schemas for API requests/responses <br>
│   └── tests/                   # Unit and integration tests <br>
│       ├── test_ingestion.py <br>
│       ├── test_extraction.py <br>
│       └── test_rag.py <br>
├── scripts/                     # Helper scripts <br>
│   ├── setup_db.py              # Script to initialize the database <br>
│   └── deploy.sh                # Deployment script <br>
├── .env                         # Environment variables (ignored in Git) <br>
├── .gitignore                   # Git ignore file <br>
├── README.md                    # Project overview and setup instructions <br>
├── requirements.txt             # Production dependencies <br>
├── requirements-dev.txt         # Development dependencies <br>
├── pyproject.toml               # Build system configuration (optional) <br>
└── Dockerfile                   # Dockerfile for containerization <br>