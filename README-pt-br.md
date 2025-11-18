# IBM HR Analytics

Este projeto implementa um pipeline ETL orquestrado pelo **Apache Airflow**, utilizando **Python** para o processamento de dados, com armazenamento intermediário no **Oracle Cloud Object Storage** e saída final em **CSV** para análise e geração de relatórios.

---

![Pipeline Architecture](architecture-2.png)

## Pipeline Flow

### 1. Extração
DAG extract_to_bucket.py realiza o download do conjunto de dados do Kaggle utilizando a biblioteca kagglehub.
Os arquivos são descompactados e copiados para o bucket no Oracle Cloud Object Storage, no diretório data/raw.

### 2. Armazenamento em Nuvem
Dados são enviados para **Oracle Cloud Object Storage**.

### 3. Processamento de ETL
**Apache Airflow** orquestra as tarefas de ETL. 
Processamentos das camadas **Bronze → Silver → Gold** é realizado utilizando **Databricks**.  
O resultado final é salvo no arquivo attrition_metrics.csv em um Bucket da Oracle.

### 4. Análise
O arquivo CSV pode ser importado em ferramentas de visualização e análise.

---

## Como começar

1. **Clone o repositório e configure as variáveis de ambiente:**
   - Preencha os arquivos .env-example e terraform-example.tfvars com suas credenciais.

2. **Inicie os contêineres:**
   ```sh
   docker-compose up -d
   ```

3. **Acesse a interface Airflow UI:**
   - Disponível em [http://localhost:8080](http://localhost:8080)


## Provisionamento de Infraestrutura com Terraform (OCI)

Este diretório **`infra`** contém os arquivos necessários para **provisionar a infraestrutura na Oracle Cloud Infrastructure (OCI)** utilizando o **Terraform**.

---

## Pré-requisitos

Antes de começar, verifique se os seguintes itens estão configurados corretamente:

### Passos
1. Instalar o Terraform
Certifique-se de que o **Terraform** está instalado em seu sistema.  
Você pode baixá-lo através do site oficial:  
👉 [https://developer.hashicorp.com/terraform/downloads](https://developer.hashicorp.com/terraform/downloads)

Para confirmar a instalação, execute:

```bash
terraform -version
```

2. Configurar credenciais OCI
O Terraform utiliza as credenciais da Oracle Cloud Infrastructure para autenticação.
Verifique se o arquivo de configuração da OCI (~/.oci/config) está devidamente configurado

Exemplo de configuração:
```` bash
[DEFAULT]
tenancy = ocid1.tenancy.oc1..aaaaaaaexample
user = ocid1.user.oc1..aaaaaaaexample
fingerprint = 20:3b:97:13:55:1c:aa:example
key_file = /home/usuario/.oci/oci_api_key.pem
region = sa-saopaulo-1
````
3. Definir variáveis obrigatórias

As variáveis necessárias estão descritas no arquivo variables-example.tf.

4. Inicialize o terraform

```bash
terraform init
```


```bash
terraform plan
```

```bash
terraform apply
```

