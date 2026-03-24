# Anhangua - Monitoramento de Alertas da Amazônia

O **Anhangua** é um backend desenvolvido em Python/FastAPI projetado para agregar, processar e servir dados de alertas de desmatamento e focos de incêndio na bacia Amazônica.

O sistema integra dados de múltiplas fontes globais, aplica algoritmos de clusterização para identificar áreas críticas e expõe esses dados através de uma API REST para consumo em interfaces de mapa.

## 🚀 Funcionalidades

*   **Agregação de Fontes:**
    *   **GLAD-L (Landsat):** Alertas de desmatamento via Global Forest Watch (GFW).
    *   **NASA FIRMS:** Focos de calor e incêndio em tempo real.
    *   **Landsat Service:** Processamento específico para alertas derivados de imagens Landsat.
*   **Processamento Inteligente:**
    *   **Clusterização (DBSCAN):** Agrupa alertas geograficamente próximos para identificar "manchas" de desmatamento em vez de pontos isolados.
    *   **Filtragem:** Filtros por data, confiança (confidence), bounding box e região (Amazônia Legal).
    *   **Cache:** Implementação de TTL Cache para otimizar requisições às APIs externas e evitar rate limits.
*   **API REST:** Endpoints rápidos construídos com FastAPI.
*   **Exportação:** Capacidade de gerar arquivos GeoJSON, CSV e KML via script de monitoramento.

## 📦 Estrutura do Projeto

*   `api.py`: Aplicação principal FastAPI. Define as rotas e o servidor web.
*   `alerts_service.py`: Camada de serviço que orquestra a unificação de dados do GFW, FIRMS e Landsat para o frontend.
*   `gfw_alerts.py`: Integração com a API do Global Forest Watch (GLAD Alerts), incluindo lógica de geostore e clusterização.
*   `firms_alerts.py`: Integração com a API da NASA FIRMS para dados de incêndio.
*   `landsat_service.py`: Wrapper específico para tratamento de dados Landsat.
*   `inspect_fields.py`: Script utilitário para inspecionar a estrutura de dados retornada pela API do GFW (debug).

## 🛠️ Instalação e Configuração

### Pré-requisitos

*   Python 3.8+
*   `pip`

### Instalação das Dependências

Instale as bibliotecas necessárias (FastAPI, Uvicorn, Requests, Scikit-learn, Numpy, etc):

```bash
pip install fastapi uvicorn requests python-dotenv scikit-learn numpy
```

### Configuração de Variáveis de Ambiente (.env)

Crie um arquivo `.env` na raiz do projeto e adicione suas chaves de API:

```ini
# Chave para API do Global Forest Watch (Necessária para /query/json)
GFW_API_TOKEN=sua_chave_gfw_aqui

# Chave para API da NASA FIRMS
FIRMS_API_TOKEN=sua_chave_firms_aqui

# (Opcional) Configurações de CORS
ALLOW_ALL_CORS=true
```

## ▶️ Executando o Projeto

Existem dois modos principais de operação:

### 1. Servidor API (Recomendado)

Para iniciar o servidor FastAPI que serve os dados para o frontend:

```bash
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Acesse a documentação interativa em: `http://localhost:8000/docs`

### 2. Monitoramento Standalone

Para rodar o script que busca novos alertas continuamente, salva históricos e gera arquivos estáticos (CSV/KML/GeoJSON) localmente:

```bash
python gfw_alerts.py
```

## 📡 Principais Endpoints

### Alertas Unificados (Mapa)
Retorna alertas normalizados de todas as fontes (GFW + FIRMS + Landsat).

`GET /alertas/mapa`
*   **Parâmetros:**
    *   `days`: (int) Dias passados para busca (padrão: 14).
    *   `confidence`: (string) Filtro de confiança (low, nominal, high).
    *   `start_date` / `end_date`: (YYYY-MM-DD) Intervalo de datas.

### Clusters de Desmatamento
Retorna áreas agrupadas (clusters) prontas para visualização, úteis para ver a densidade do desmatamento.

`GET /alertas/mapa/clusters`
*   **Parâmetros:**
    *   `eps_km`: (float) Raio de distância para agrupar pontos (padrão: 1.0 km).
    *   `min_samples`: (int) Mínimo de alertas para formar um cluster.

### Alertas FIRMS (Fogo)
Retorna apenas focos de calor/incêndio.

`GET /alertas/firms`

### Alertas Landsat (Raw)
Retorna dados brutos filtrados por bounding box.

`GET /alertas/landsat`

## 📝 Logs

Os logs da aplicação são salvos automaticamente no diretório `logs/alerts_api.log`.

---
Projeto Anhangua.

