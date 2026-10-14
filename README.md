# 🛒 E-commerce Checkout API

API de checkout para e-commerce desenvolvida como parte de um desafio prático da **DIO (Digital Innovation One)**.

A API recebe um pedido de checkout, persiste no PostgreSQL com auditoria (Hibernate Envers) e publica um evento no Kafka. Ela também consome eventos de pagamento aprovado e atualiza o status do checkout.

> O curso original usa o modelo antigo do Spring Cloud Stream (`@EnableBinding`, `@Output`, `@Input`, `@StreamListener`) com Avro. Este projeto foi **migrado para o modelo funcional** (`StreamBridge` + beans `Consumer`) e usa **JSON** nas mensagens, rodando em Spring Boot 3 e Java 17.

---

## 📌 Sumário

- [Tecnologias](#-tecnologias)
- [Arquitetura e fluxo](#-arquitetura-e-fluxo)
- [Pré-requisitos](#-pré-requisitos)
- [Como rodar](#-como-rodar)
- [Endpoints](#-endpoints)
- [Eventos Kafka](#-eventos-kafka)
- [Como testar o fluxo completo](#-como-testar-o-fluxo-completo)
- [Configuração](#-configuração)
- [Estrutura do projeto](#-estrutura-do-projeto)
- [Decisões técnicas](#-decisões-técnicas)
- [Autor](#-autor)

---

## 🧰 Tecnologias

- **Java 17**
- **Spring Boot 3.3.7** (Web, Data JPA)
- **Spring Cloud Stream 4.x** (Spring Cloud 2023.0.3) com binder Kafka
- **Apache Kafka** (Confluent Platform) + Zookeeper
- **PostgreSQL**
- **Hibernate Envers** (auditoria das entidades)
- **Lombok**
- **Maven** (com Maven Wrapper)
- **Docker / Docker Compose**

---

## 🧭 Arquitetura e fluxo

```
Cliente ──POST /v1/checkout──▶ Checkout API ──▶ PostgreSQL (checkout)
                                    │
                                    └──▶ Kafka: streaming.ecommerce.checkout.created
                                                         │
                                                  (serviço de pagamento)
                                                         │
Kafka: streaming.ecommerce.payment.paid ──▶ Checkout API (status → APPROVED)
```

1. O cliente envia o pedido para `POST /v1/checkout`.
2. A API salva o checkout com status `CREATED` e devolve o `code` gerado.
3. Um evento `CheckoutCreatedEvent` é publicado no tópico `streaming.ecommerce.checkout.created`.
4. Quando um pagamento é aprovado, o evento chega no tópico `streaming.ecommerce.payment.paid`.
5. O consumidor `paymentPaid` atualiza o checkout para `APPROVED`.

---

## ✅ Pré-requisitos

- [JDK 17](https://adoptium.net/)
- [Docker](https://www.docker.com/) e Docker Compose
- Maven (opcional, o projeto inclui o `mvnw`)

---

## 🚀 Como rodar

### 1. Clone o repositório

```bash
git clone https://github.com/fxxggah/ecommerce-checkout-api.git
cd ecommerce-checkout-api
```

### 2. Suba a infraestrutura (Postgres, Kafka, Zookeeper)

```bash
cd docker
docker compose up -d
cd ..
```

Serviços criados pelo `docker-compose.yml`:

| Serviço           | Container         | Porta(s)                |
|-------------------|-------------------|-------------------------|
| PostgreSQL (checkout) | `db-checkout` | `5432`                  |
| PostgreSQL (payment)  | `db-payment`  | `5433`                  |
| Zookeeper         | `zookeeper`       | `2181`                  |
| Kafka             | `broker`          | `9092` (host), `29092` (interno) |
| Schema Registry   | `schema-registry` | `8081` (não usado no fluxo atual com JSON) |

> Aguarde alguns segundos para o Kafka terminar de subir antes de iniciar a aplicação.

### 3. Rode a aplicação

Linux / macOS:

```bash
./mvnw spring-boot:run
```

Windows (PowerShell):

```powershell
.\mvnw.cmd spring-boot:run
```

A API sobe em **http://localhost:8085**.

---

## 🔌 Endpoints

### `POST /v1/checkout`

Cria um novo checkout.

**Request body**

```json
{
  "firstName": "Maria",
  "lastName": "Silva",
  "email": "maria@email.com",
  "address": "Rua das Flores, 123",
  "complement": "Apto 45",
  "country": "Brasil",
  "state": "SP",
  "cep": "18600-000",
  "saveAddress": true,
  "saveInfo": false,
  "paymentMethod": "credit_card",
  "cardName": "MARIA SILVA",
  "cardNumber": "0000000000000000",
  "cardDate": "12/30",
  "cardCvv": "000",
  "products": ["produto-1", "produto-2"]
}
```

**Response** — `201 Created`

```json
{
  "code": "c3b5f1d2-8a4e-4c5b-9d3a-1f2e3d4c5b6a"
}
```

**Exemplo com curl**

```bash
curl -X POST http://localhost:8085/v1/checkout \
  -H "Content-Type: application/json" \
  -d '{
    "firstName": "Maria",
    "lastName": "Silva",
    "email": "maria@email.com",
    "address": "Rua das Flores, 123",
    "complement": "Apto 45",
    "country": "Brasil",
    "state": "SP",
    "cep": "18600-000",
    "saveAddress": true,
    "saveInfo": false,
    "paymentMethod": "credit_card",
    "cardName": "MARIA SILVA",
    "cardNumber": "0000000000000000",
    "cardDate": "12/30",
    "cardCvv": "000",
    "products": ["produto-1", "produto-2"]
  }'
```

> ⚠️ Os dados de cartão chegam no request, mas **não são persistidos** pela entidade `Checkout`. Use apenas dados fictícios nos testes.

---

## 📨 Eventos Kafka

| Direção   | Tópico                                | Binding                  | Payload (JSON)                                   |
|-----------|---------------------------------------|--------------------------|--------------------------------------------------|
| Publica   | `streaming.ecommerce.checkout.created` | `checkout-created-output` | `{"checkoutCode":"...","status":"CREATED"}`      |
| Consome   | `streaming.ecommerce.payment.paid`     | `paymentPaid-in-0`        | `{"checkoutCode":"..."}`                         |

O consumidor usa o grupo `checkout-api`.

---

## 🧪 Como testar o fluxo completo

**1. Crie um checkout** (veja o `curl` acima) e guarde o `code` retornado.

**2. Confira o evento publicado:**

```bash
docker exec broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic streaming.ecommerce.checkout.created \
  --from-beginning
```

Saída esperada:

```json
{"checkoutCode":"c3b5f1d2-...","status":"CREATED"}
```

**3. Simule um pagamento aprovado** publicando no tópico de pagamento:

```bash
docker exec -it broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic streaming.ecommerce.payment.paid
```

Cole a mensagem (troque pelo `code` real) e dê Enter:

```json
{"checkoutCode":"c3b5f1d2-..."}
```

**4. Verifique no banco** que o status virou `APPROVED`:

```bash
docker exec -it db-checkout psql -U admin -d checkout \
  -c "select code, status from checkout;"
```

---

## ⚙️ Configuração

Principais propriedades em `src/main/resources/application.properties`:

| Propriedade                                   | Valor padrão                                  |
|-----------------------------------------------|-----------------------------------------------|
| `server.port`                                 | `8085`                                        |
| `spring.datasource.url`                       | `jdbc:postgresql://localhost:5432/checkout`   |
| `spring.datasource.username` / `password`     | `admin` / `admin`                             |
| `spring.jpa.hibernate.ddl-auto`               | `update`                                      |
| `spring.cloud.stream.kafka.binder.brokers`    | `localhost:9092`                              |
| `spring.cloud.function.definition`            | `paymentPaid`                                 |

As credenciais acima são para **ambiente local de estudo**. Em qualquer ambiente real, use variáveis de ambiente ou um gerenciador de segredos.

---

## 🗂️ Estrutura do projeto

```
ecommerce-checkout-api
├── docker/                    # docker-compose (Postgres, Kafka, Zookeeper, Schema Registry)
├── src/main/java/dio/ecommerce/checkout
│   ├── CheckoutApplication.java
│   ├── controller/checkout/   # CheckoutController, CheckoutRequest, CheckoutResponse
│   ├── service/               # CheckoutService e implementação
│   ├── repository/            # CheckoutRepository (Spring Data JPA)
│   ├── entity/                # Checkout, CheckoutItem, Shipping
│   ├── event/                 # CheckoutCreatedEvent, PaymentCreatedEvent
│   ├── listener/              # PaymentPaidListener (consumidor Kafka)
│   └── util/                  # UUIDUtil
├── src/main/resources/        # application.properties
├── pom.xml
└── mvnw / mvnw.cmd
```

---

## 🧠 Decisões técnicas

- **Spring Cloud Stream funcional:** o modelo antigo (`@EnableBinding`, `@Output`, `@Input`, `@StreamListener`) foi removido no Spring Cloud Stream 4.x. A publicação agora usa `StreamBridge` e o consumo usa um bean `Consumer<PaymentCreatedEvent>`.
- **JSON no lugar de Avro:** elimina a necessidade de plugin Avro, dependências da Confluent e Schema Registry para este escopo.
- **Binder `kafka` explícito:** o projeto tem também o `spring-cloud-stream-binder-kafka-streams` no classpath, então cada binding declara `binder=kafka` para evitar ambiguidade.
- **Auditoria com Envers:** a entidade `Checkout` é anotada com `@Audited`, mantendo histórico de alterações.
- **`updateStatus` seguro:** só atualiza se o checkout existir e respeita o status recebido (o código original do curso salvava um registro vazio quando o código não era encontrado).

---

## 👤 Autor

Desenvolvido por **Biel** — [@fxxggah](https://github.com/fxxggah)

Projeto de estudo realizado no desafio prático da [DIO](https://www.dio.me/).
