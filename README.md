# Game Data Assignment

## 📌 Project Description

This project provides **Apache Airflow** based ETL pipelines to process game event data. Player actions are incrementally loaded daily and transformed into analytics tables in **Google BigQuery** with built-in data quality checks.

---

## ⚙️ Technologies Used

- **Apache Airflow**: For managing ETL workflows  
- **Python**: For data processing and transformations  
- **Google BigQuery**: For data storage and analytics  
- **Git**: Version control  

---

## 🗂️ Project Structure

    ```plaintext
    game_data_assignment/
    ├── dag_fmg.py               # Airflow DAG definition  
    ├── files_to_deploy.cfg      # Configuration file  
    ├── requirements.txt         # Python dependencies  
    ├── .gitattributes           # Git attributes  
    ├── .idea/                   # IDE project files  
    ├── fmg_packages/            # Custom Python packages  
    └── test/                    # Test files  

---

## ⚙️ Setup  

1. **Clone and setup**:
   ```bash
   git clone https://github.com/efesabanogluu/game_data_assignment.git
   cd game_data_assignment

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt

3. **Initialize Airflow**:
   ```bash
   airflow db init
   airflow users create --username admin --password admin --role Admin

4. **Run services**:
   ```bash
   airflow webserver --port 8080 &
   airflow scheduler

---

## ✅ Features
Incremental Daily Loads: Only new data is processed to reduce load on source databases.

Data Quality Checks: Predefined checks ensure data accuracy and integrity.

Modular Design: Code is organized for reusability and easy maintenance.

Testable Code: Included tests ensure code correctness and reliability.

---

## 🧪 Testing
The project includes testing framework details:

PyTest: For unit testing
unittest: Python’s built-in testing module

Run tests with:
    
    pytest

---

## 📈 Use Cases
This ETL pipeline suits data engineers and analysts working on game analytics, such as:

User Behavior Analysis: Analyzing player activities within the game.

Performance Monitoring: Monitoring game server performance metrics.

Revenue Analysis: Analyzing in-game purchase data.
