# JobBot

JobBot is an automated tool designed to search for remote jobs, filter them based on your preferences, score them using AI, and notify you of the best matches.

## Prerequisites

- **Python 3.10+**
- A Groq API key (for AI scoring)
- Gmail account with App Password (for email notifications)
- Telegram Bot (for Telegram notifications)

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd JobBot
   ```

2. **Set up a virtual environment (optional but recommended):**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure the application:**
   - Copy `.env.example` to `.env` and fill in your API keys.
   - Edit `config.yaml` to set your job search criteria, skills, and notification preferences.

## Usage

Run the main script to start searching for jobs:

```bash
python main.py
```

## Scheduling

To run JobBot daily at a specific time, you can use the built-in scheduler or set up a Cron job (Linux) / Task Scheduler (Windows).

The `config.yaml` includes a `schedule_time` setting that is used by the internal scheduler if you keep the script running.

## Project Structure

- `main.py`: Entry point for the application.
- `config.py`: Configuration loader and validator.
- `modules/`: Core logic modules (scraper, filters, etc.).
- `output/`: Folder where job search results (CSVs) are saved.
- `logs/`: Application logs.
