# 🤖 JobBot: Remote Job Search Automation

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://makeapullrequest.com)

**Stop manually scrolling through job boards.** JobBot is a production-grade automated pipeline that scrapes 5+ major job boards simultaneously, applies smart filtering, and uses AI to score how well you match a job description—all delivered straight to your inbox and Telegram daily.

---

### 🚀 Key Features

*   ✅ **Multi-Board Scraping**: Aggregates listings from LinkedIn, Indeed, ZipRecruiter, Glassdoor, and more.
*   ✅ **Smart Filtering**: Filter by remote-only status, specific skills, salary minimums, and company blacklists.
*   ✅ **AI-Powered Matching**: Uses free high-reasoning models (via Nvidia/Groq) to score jobs from 0-100% based on your profile.
*   ✅ **Automated Tracking**: Automatically updates a Google Sheets tracker with new finds, AI scores, and application status.
*   ✅ **Instant Notifications**: Receive clean HTML email digests and mobile Telegram alerts.
*   ✅ **100% Free Setup**: Built using open-source libraries and free-tier API providers.

---

### 📦 Quick Start (5-Minute Setup)

1.  **Clone & Install**
    ```bash
    git clone https://github.com/yourusername/JobBot.git
    cd JobBot
    python -m venv .venv
    source .venv/bin/activate  # Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    ```

2.  **Config & Keys**
    *   Copy `.env.example` to `.env` and add your API keys.
    *   Edit `config.yaml` with your job titles and desired skills.

3.  **Run It**
    ```bash
    python main.py --now
    ```

---

### 🛠️ Detailed Configuration Guide (`config.yaml`)

| Field | Description | Example |
| :--- | :--- | :--- |
| `search_terms` | List of job titles to search for. | `["Python Developer", "Data Scientist"]` |
| `skills` | Keywords that MUST be in the job description. | `["django", "aws", "kubernetes"]` |
| `min_salary` | Minimum annual salary (numeric). | `80000` |
| `job_type` | Filter by `full-time`, `contract`, etc. | `"full-time"` |
| `hours_old` | Only fetch jobs posted in the last N hours. | `24` |
| `blacklisted_companies` | Skip these annoying companies. | `["Revature", "CyberCoders"]` |
| `ai_scoring` | Enable/Disable AI matching engine. | `enabled: true` |

---

### 🔌 Setup Guides

#### 1. AI Scoring (NVIDIA API - Highly Recommended)
1.  Visit [build.nvidia.com](https://build.nvidia.com/).
2.  Sign up for a free developer account (includes 10k free credits).
3.  Generate an API Key and add it to `.env` as `NVIDIA_API_KEY`.

#### 2. Gmail Notifications (App Password)
1.  Go to your [Google Account Settings](https://myaccount.google.com/).
2.  Enable **2-Step Verification**.
3.  Search for **App Passwords**.
4.  Generate one for "Mail" and "Other (JobBot)". Copy the 16-character code to `.env` as `GMAIL_APP_PASSWORD`.

#### 3. Telegram Bot Alerts
1.  Message [@BotFather](https://t.me/botfather) on Telegram and type `/newbot`.
2.  Follow instructions to get your **Bot Token**.
3.  Message [@userinfobot](https://t.me/userinfobot) to get your **Chat ID**.
4.  Add both to `.env`.

#### 4. Google Sheets Tracking
1.  Go to [Google Cloud Console](https://console.cloud.google.com/).
2.  Create a project and enable the **Google Sheets API**.
3.  Create a **Service Account**, download the JSON key, and rename it to `credentials.json` in the root folder.
4.  Share your Google Sheet with the email address of the service account.

---

### 🎮 Usage Examples

*   `python main.py --now` : Standard run (Scrape -> Filter -> Score -> Export).
*   `python main.py --schedule` : Keeps the script running and triggers daily at the time in `config.yaml`.
*   `python main.py --health` : **Dashboard** showing if all your APIs and job boards are working.
*   `python main.py --stats` : Pulls live stats from your Google Sheet (Total Found vs. High Match).
*   `python main.py --test` : Quickly test the pipeline with only 5 results (saves API credits).
*   `python main.py --no-ai`: Skip the AI scoring step to run faster.

---

### ☁️ 24/7 Automation (GitHub Actions)

You can run JobBot automatically every day for free using GitHub Actions.

#### 1. Fork the Repository
Click the **Fork** button at the top right of this page to create your own copy of JobBot.

#### 2. Configure GitHub Secrets
Go to your forked repository's **Settings > Secrets and variables > Actions** and click **New repository secret**. Add the following:

| Secret Name | Description |
| :--- | :--- |
| `NVIDIA_API_KEY` | Your Nvidia/Groq API key. |
| `GMAIL_ADDRESS` | Your Gmail address. |
| `GMAIL_APP_PASSWORD` | The 16-character App Password. |
| `TELEGRAM_BOT_TOKEN` | Your Telegram Bot Token. |
| `TELEGRAM_CHAT_ID` | Your Telegram Chat ID. |
| `GOOGLE_SHEET_NAME` | The exact name of your Google Sheet. |
| `GOOGLE_SHEETS_CRED_FILE_B64` | The contents of your `credentials.json` (base64 encoded). |

> [!TIP]
> **How to encode your credentials to Base64?**
> - **Windows (PowerShell)**: `[Convert]::ToBase64String([IO.File]::ReadAllBytes('credentials.json'))`
> - **Linux/macOS**: `base64 -i credentials.json` (then copy the output).

#### 3. Enable the Workflow
1.  Go to the **Actions** tab in your GitHub repository.
2.  Select **JobBot Automation** from the left sidebar.
3.  Click **Enable workflow**.
4.  (Optional) Click **Run workflow > Run workflow** to test it immediately!

---

### 🏗️ Architecture

```text
    [ Job Boards ] --> ( Scraper ) --> [ Raw Data (CSV) ]
                             |
                   ( Filter Engine ) --> [ Matching Jobs ]
                             |
                    ( AI Scorer ) ----> ( Nvidia API )
                             |
           +-----------------+-----------------+
           |                 |                 |
    ( Notifier )      ( Exporter )      ( Google Sheets )
    [ Email/TG ]    [ latest_jobs.csv ]   [ Tracker App ]
```

---

### ❓ FAQ

**Q: Is this legal?**
A: Yes. JobBot uses public-facing web data via the `jobspy` library. However, always respect the Terms of Service of individual job boards. Use the `hours_old` and `results_per_site` filters to keep your request volume reasonable.

**Q: How do I run this 24/7 for free?**
A: We've included a GitHub Actions workflow in `.github/workflows/jobbot.yml`. Follow the **24/7 Automation** section above to set up your secrets and schedule daily runs.

**Q: Can I use different AI models?**
A: Absolutely. Check `config.yaml` under `ai_scoring.model`. You can swap to any model supported by the NVIDIA or OpenAI-compatible endpoints.

---

### 📄 License

This project is licensed under the MIT License - see the `LICENSE` file for details.

---

⭐ **Found this useful? Give it a star to help others find it!**
