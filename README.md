# Largest Banks by Market Capitalization ETL Project

## **Project Overview**

This project demonstrates the application of the ETL (Extraction, Transformation, Loading) process using real-world data. It involves compiling a list of the top 10 largest banks in the world by market capitalization (in billion USD), transforming the data to include market capitalization in GBP, EUR, and INR, and saving the data in both CSV and SQLite database formats. This script is designed to automate the process so that it can be executed quarterly.

---

## **Project Features**
1. Extracts bank data from a web page.
2. Transforms the data to include market capitalizations in GBP, EUR, and INR using live exchange rates.
3. Saves the transformed data to:
   - A CSV file.
   - A SQLite database.
4. Logs progress at each stage in a `code_log.txt` file.
5. Queries the database to generate insights.

---

## **Project Scenario**

I am tasked with:
- Extracting bank data from a Wikipedia page.
- Adding currency conversion values using provided exchange rates.
- Saving the transformed data into CSV and SQLite database formats.
- Logging the progress of all operations for debugging and monitoring purposes.

---
## **Input Data**
### **1. Source Data**
- **URL**: [Largest Banks by Market Capitalization (Web Archive)](https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks)

### **2. Exchange Rates**
- **CSV Path**: [Exchange Rates File](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv)

---

## **Output Data**
1. **CSV File**:  
   `Largest_banks_data.csv`  
   A file containing the top 10 banks' market capitalizations in multiple currencies.

2. **Database**:  
   `Banks.db`  
   SQLite database storing the transformed data in the table `Largest_banks`.

3. **Log File**:  
   `code_log.txt`  
   File logging each stage of the ETL process.

---

## **Tasks Overview**

### **Task 1: Logging**
- Logs progress of the ETL process to the file `code_log.txt`.

### **Task 2: Extract Data**
- Extracts the bank data table from the source URL and saves it to a Pandas DataFrame.

### **Task 3: Transform Data**
- Adds new columns for Market Capitalization in GBP, EUR, and INR using exchange rates.

### **Task 4: Load to CSV**
- Saves the transformed data to a CSV file.

### **Task 5: Load to Database**
- Saves the transformed data into a SQLite database table.

### **Task 6: Query the Database**
- Executes SQL queries on the database to generate insights.

---

## **Usage Instructions**

1. **Run the ETL Script**
   Execute the script `banks_project.py` using Python.

   ```bash
   python banks_project.py
   ```
2. **Outputs**
   
   Check Largest_banks_data.csv for the transformed data.

   View the database Banks.db for structured storage.

   Open code_log.txt for process logs.
   
3. **Run Custom Queries**

   Add or modify queries in the run_queries() function.

 ## **Example Outputs**

### **CSV Output (Largest_banks_data.csv)**
```csv
Name,MC_USD_Billion,MC_GBP_Billion,MC_EUR_Billion,MC_INR_Billion
Bank A,500,400,450,37500
Bank B,450,360,405,33750
```

## **Log File (code_log.txt)**

The log file captures the progress of the ETL process with timestamps at various stages:

```plaintext
2024-Nov-17-10:35:01: Preliminaries complete. Initiating ETL process.
2024-Nov-17-10:36:05: Data extraction complete. Initiating Transformation process.
2024-Nov-17-10:37:12: Data saved to CSV file.
2024-Nov-17-10:37:45: Data loaded to Database as table. Running the query.
2024-Nov-17-10:38:15: Process Complete.
```

