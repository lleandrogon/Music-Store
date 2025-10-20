# 🎵 Music Store – ETL e Automação no Databricks

## 📖 Sobre o Projeto  
O **Music Store** é um projeto de **ETL (Extract, Transform, Load)** desenvolvido no **Databricks**, com o objetivo de processar e analisar dados de uma loja de música.  
O pipeline automatiza o tratamento de dados referentes a **artistas, vendas, contas, gêneros e músicas**, garantindo integridade, padronização e agilidade nas análises.  

Além do fluxo de ETL, o projeto também inclui **Jobs agendados** que executam os scripts automaticamente e **notificam por e-mail** em caso de falhas, proporcionando um processo confiável e monitorável de ponta a ponta.

---

## ⚙️ Tecnologias Utilizadas
- **Databricks** (Notebook e Jobs)
- **Apache Spark (PySpark)**
- **Delta Lake**
- **Python**
- **SQL**
- **SMTP** (para envio de notificações por e-mail)

---

## 🧩 Estrutura do Projeto

| Etapa | Descrição |
|-------|------------|
| **1. Extração** | Leitura de arquivos brutos contendo informações sobre artistas, vendas, contas e músicas. |
| **2. Transformação** | Padronização e limpeza dos dados, conversão para formato **Delta** e criação de tabelas analíticas. |
| **3. Carga (Load)** | Armazenamento dos dados tratados em tabelas Delta dentro do Databricks. |
| **4. Automação** | Criação de **Jobs agendados** para execução periódica dos notebooks. |
| **5. Monitoramento** | Envio de **notificações por e-mail** em caso de falhas na execução dos Jobs. |

---

## 📊 Funcionalidades Principais

- 🎤 **Performance de artistas**  
  Análise de desempenho e popularidade de artistas.  

- 💿 **Quantidade de músicas por gênero**  
  Indicadores de diversidade e volume musical por categoria.  

- 🧾 **Vendas por artista e por cliente**  
  Insights financeiros e comportamentais sobre os consumidores.  

- 🕒 **Agendamento automático de tarefas**  
  Jobs configurados para execução diária no Databricks.  

- 📧 **Notificações automáticas de erro**  
  Envio de e-mails automáticos ao detectar falhas na execução dos scripts.

---

## 🚀 Como Executar

1. **Importe** os notebooks no ambiente **Databricks**.  
2. **Configure** as conexões de entrada (dados brutos) e saída (armazenamento Delta).  
3. **Agende** os notebooks como **Jobs** no Databricks, definindo frequência e notificações.  
4. **Monitore** os resultados e logs diretamente pela interface do Databricks.  

---

## 🧠 Aprendizados e Destaques

- Implementação completa de um pipeline **ETL escalável** no Databricks.  
- Utilização do **formato Delta** para otimizar consultas e versionamento de dados.  
- Criação de **Jobs automatizados** com alertas por e-mail.  
- Melhoria da governança e rastreabilidade dos dados.
