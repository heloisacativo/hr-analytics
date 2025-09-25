# IBM HR Analytics

This project implements an ETL pipeline orchestrated by **Apache Airflow**, using **Python** and **pandas** for data processing, with intermediate storage in **Oracle Cloud Object Storage** and final output as **CSV** for analysis in **Power BI**.

---

![Pipeline Architecture](architecture-2.png)

## Pipeline Flow

### 1. Extraction
The `extract.py` script downloads the dataset from Kaggle using the `kagglehub` library.  
Files are unzipped/copied to the `data/raw` directory.

### 2. Cloud Storage
Data can be uploaded to **Oracle Cloud Object Storage**.

### 3. Processing (ETL)
**Apache Airflow** orchestrates the ETL tasks.  
Processing of the **Bronze → Silver → Gold** layers is performed using **pandas**.  
The final result is saved in the `attrition_metrics.csv` file.

### 4. Analysis
The CSV file can be imported directly into **Power BI** for visualization and analysis.

---

## Quick Start

1. **Clone the repository and set up environment variables:**
   - Fill in the `.env` and `terraform-example.tfvars` files with your credentials.

2. **Start the containers:**
   ```sh
   docker-compose up -d
   ```

3. **Run the extraction script:**
   ```sh
   python data/extract/extract.py
   ```

4. **Access Airflow UI:**
   - Available at [http://localhost:8080](http://localhost:8080)

5. **Trigger the `transform_gold` DAG:**
   - This will generate the final CSV file.

6. **Open Power BI:**
   - Import the file `data/gold/attrition_metrics.csv` to create your dashboards.

---

## Fluxo do Pipeline

### 1. Extração
O script `extract.py` realiza o download do conjunto de dados do Kaggle utilizando a biblioteca `kagglehub`.  
Os arquivos são descompactados/copiados para o diretório `data/raw`.

### 2. Armazenamento em Nuvem
Os dados podem ser enviados para o **Oracle Cloud Object Storage**.

### 3. Processamento (ETL)
O **Apache Airflow** orquestra as tarefas de ETL.  
O processamento das camadas **Bronze → Silver → Gold** é executado utilizando a biblioteca **pandas**.  
O resultado final é salvo no arquivo `attrition_metrics.csv`.

### 4. Análise
O arquivo CSV pode ser importado diretamente no **Power BI** para visualização e análise.

---

## Como Executar

1. **Clonar o repositório e configurar as variáveis de ambiente:**
   - Preencha o arquivo `.env` e `terraform-example.tfvars` com as credenciais.

2. **Iniciar os contêineres:**
   ```sh
   docker-compose up -d
   ```

3. **Executar o script de extração:**
   ```sh
   python data/extract/extract.py
   ```

4. **Acessar a interface do Airflow:**
   - Disponível em [http://localhost:8080](http://localhost:8080)

5. **Executar o DAG `transform_gold`:**
   - Este procedimento gerará o arquivo CSV final.

6. **Abrir o Power BI:**
   - Importe o arquivo `data/gold/attrition_metrics.csv` para a criação dos painéis de controle.