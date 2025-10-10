# IBM HR Analytics

This project implements an ETL pipeline orchestrated by **Apache Airflow**, using **Python** for data processing, with intermediate storage in **Oracle Cloud Object Storage** and final output as **CSV** for analysis in **Power BI**.

---

![Pipeline Architecture](architecture-2.png)

## Pipeline Flow

### 1. Extraction
The `extract_to_bucket.py` DAG downloads the dataset from Kaggle using the `kagglehub` library.  
Files are unzipped and copied to the Oracle Cloud Object Storage bucket in the `data/raw` directory.

### 2. Cloud Storage
Data can be uploaded to **Oracle Cloud Object Storage**.

### 3. Processing (ETL)
**Apache Airflow** orchestrates the ETL tasks.  
Processing of the **Bronze → Silver → Gold** layers is performed using **Databricks**.  
The final result is saved in the `attrition_metrics.csv` file.

### 4. Analysis
The CSV file can be imported directly into **Power BI** for visualization and analysis.

---

## Quick Start

1. **Clone the repository and set up environment variables:**
   - Fill in the `.env-example` and `terraform-example.tfvars` files with your credentials.

2. **Start the containers:**
   ```sh
   docker-compose up -d
   ```

3. **Access Airflow UI:**
   - Available at [http://localhost:8080](http://localhost:8080)
   
   

## Infrastructure Provisioning with Terraform (OCI)

   This **`infra`** directory contains the necessary files to **provision infrastructure on Oracle Cloud Infrastructure (OCI)** using **Terraform**.

   ---
   ### Prerequisites

   Before you begin, make sure the following items are properly configured:

   ### Steps

1. Install Terraform
   Ensure **Terraform** is installed on your system.  
   You can download it from the official website:  
   👉 [https://developer.hashicorp.com/terraform/downloads](https://developer.hashicorp.com/terraform/downloads)

   To confirm the installation, run:

   ```bash
   terraform -version
   ```

2. Configure OCI Credentials
   Terraform uses Oracle Cloud Infrastructure credentials for authentication.  
   Make sure your OCI configuration file (`~/.oci/config`) is properly set up.

   Example configuration:
   ```bash
   [DEFAULT]
   tenancy = ocid1.tenancy.oc1..aaaaaaaexample
   user = ocid1.user.oc1..aaaaaaaexample
   fingerprint = 20:3b:97:13:55:1c:aa:example
   key_file = /home/user/.oci/oci_api_key.pem
   region = sa-saopaulo-1
   ```
3. Set Required Variables

   The required variables are described in the `variables-example.tf` file.

4. Initialize Terraform

   ```bash
   terraform init
   ```

   ```bash
   terraform plan
   ```

   ```bash
   terraform apply
   ```



