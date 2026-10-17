# 🛒 E-commerce Checkout API

API de checkout para e-commerce desenvolvida como parte de um desafio prático da **DIO (Digital Innovation One)**.

A API recebe um pedido de checkout, persiste no PostgreSQL com auditoria (Hibernate Envers) e publica um evento no Kafka. Ela também consome eventos de pagamento aprovado, publicados pelo [payment-api](https://github.com/fxxggah/ecommerce-payment-api), e atualiza o status do checkout.

> O curso original usa o modelo antigo do Spring Cloud Stream (`@EnableBinding`, `@Output`, `@Input`, `@StreamListener`) com Avro. Este projeto foi **migrado para o modelo funcional** (`StreamBridge` + beans `Consumer`) e usa **JSON** nas mensagens, rodando em Spring Boot 3 e Java 17. Os testes de integração originais (Cucumber) também foram substituídos por **JUnit 5 + Testcontainers**.

---

## 📌 Sumário

- [Tecnologias](#-tecnologias)
- [Arquitetura e fluxo](#-arquitetura-e-fluxo)
- [Pré-requisitos](#-pré-requisitos)
- [Como rodar](#-como-rodar)
- [Endpoints](#-endpoints)
- [Eventos Kafka](#-eventos-kafka)
- [Como testar o fluxo completo](#-como-testar-o-fluxo-completo)
- [Testes automatizados](#-testes-automatizados)
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
- **JUnit 5** + **Testcontainers** (testes de integração)
- **Maven** (com Maven Wrapper)
- **Docker / Docker Compose**

---

## 🧭 Arquitetura e fluxo

```
Cliente ──POST /v1/checkout──▶ Checkout API ──▶ PostgreSQL (checkout)
                                    │
                                    └──▶ Kafka: streaming.ecommerce.checkout.created
                                                         │
                                                         ▼
                                                  Payment API
                                           (grava o pagamento no PostgreSQL)
                                                         │
Kafka: streaming.ecommerce.payment.paid ◀────────────────┘
        │
        ▼
Checkout API (status → APPROVED)
```

1. O cliente envia o pedido para `POST /v1/checkout`.
2. A API salva o checkout com status `CREATED` e devolve o `code` gerado.
3. Um evento `CheckoutCreatedEvent` é publicado no tópico `streaming.ecommerce.checkout.created`.
4. O [payment-api](https://github.com/fxxggah/ecommerce-payment-api) consome o evento, registra o pagamento e publica em `streaming.ecommerce.payment.paid`.
5. O consumidor `paymentPaid` deste serviço recebe o evento e atualiza o checkout para `APPROVED`.

---

## ✅ Pré-requisitos

- [JDK 17](https://adoptium.net/)
- [Docker](https://www.docker.com/) e Docker Compose
- Maven (opcional, o projeto inclui o `mvnw`)
- Para o fluxo completo (checkout → pagamento → aprovação): o [payment-api](https://github.com/fxxggah/ecommerce-payment-api) rodando

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

| Serviço               | Container         | Porta(s)                                   |
|-----------------------|-------------------|--------------------------------------------|
| PostgreSQL (checkout) | `db-checkout`     | `5432`                                     |
| PostgreSQL (payment)  | `db-payment`      | `5433`                                     |
| Zookeeper             | `zookeeper`       | `2181`                                     |
| Kafka                 | `broker`          | `9092` (host), `29092` (interno)           |
| Schema Registry       | `schema-registry` | `8081` (não usado no fluxo atual com JSON) |

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

> Sem o payment-api rodando, o checkout é criado normalmente, mas fica em `CREATED`, porque ninguém publica o evento de pagamento aprovado.

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

| Direção | Tópico                                 | Binding                   | Payload (JSON)                                   |
|---------|----------------------------------------|---------------------------|--------------------------------------------------|
| Publica | `streaming.ecommerce.checkout.created` | `checkout-created-output` | `{"checkoutCode":"...","status":"CREATED"}`      |
| Consome | `streaming.ecommerce.payment.paid`     | `paymentPaid-in-0`        | `{"checkoutCode":"...","paymentCode":"..."}`     |

O consumidor usa o grupo `checkout-api`.

O bean `paymentPaid` recebe a mensagem como `byte[]` e desserializa o JSON com o `ObjectMapper` do Spring, em vez de depender da conversão automática de tipos do Spring Cloud Stream.

---

## 🧪 Como testar o fluxo completo

Com a infraestrutura, o **payment-api** (porta `8082`) e este serviço (porta `8085`) rodando:

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

**3. Aguarde alguns segundos e verifique o status no banco.** O payment-api processa o evento, e o checkout deve virar `APPROVED`:

```bash
docker exec -it db-checkout psql -U admin -d checkout \
  -c "select code, status from checkout order by id desc limit 1;"
```

**Testando só este serviço (sem o payment-api):** simule o pagamento publicando o evento na mão:

```bash
docker exec -it broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic streaming.ecommerce.payment.paid
```

Cole a mensagem (troque pelo `code` real) e dê Enter:

```json
{"checkoutCode":"c3b5f1d2-...","paymentCode":"pagamento-de-teste"}
```

---

## ✅ Testes automatizados

O projeto tem um teste de integração, `CheckoutControllerTest`, que sobe a aplicação completa e valida o endpoint `POST /v1/checkout`:

- envia um checkout válido e confere que a resposta é `201 Created`;
- confere que o corpo traz o `code` gerado.

O teste usa **Testcontainers**: um PostgreSQL e um Kafka reais são criados em containers descartáveis, em portas aleatórias, e removidos ao final. Ele **não usa** os containers do `docker-compose` e não altera os seus dados locais.

**Pré-requisito:** Docker em execução.

```bash
./mvnw test
```

Windows (PowerShell):

```powershell
.\mvnw.cmd test
```

Para rodar só esse teste:

```bash
./mvnw test -Dtest=CheckoutControllerTest
```

Notas:

- A primeira execução leva cerca de 1 minuto, porque baixa as imagens `postgres:16-alpine` e `confluentinc/cp-kafka:7.5.0`. As seguintes são mais rápidas.
- Avisos como `Connection to node 1 ... could not be established` no final do log são normais: os containers são encerrados antes do consumidor Kafka terminar de desconectar.
- O Testcontainers está na versão `1.21.4`. Versões anteriores não funcionam com Docker 29 ou superior (erro `Could not find a valid Docker environment`).
- O teste cobre o endpoint e a gravação no banco. O fluxo completo com o payment-api (evento no Kafka até o status `APPROVED`) é verificado manualmente, como descrito acima.

---

## ⚙️ Configuração

Principais propriedades em `src/main/resources/application.properties`:

| Propriedade                                | Valor padrão                                |
|--------------------------------------------|---------------------------------------------|
| `server.port`                              | `8085`                                      |
| `spring.datasource.url`                    | `jdbc:postgresql://localhost:5432/checkout` |
| `spring.datasource.username` / `password`  | `admin` / `admin`                           |
| `spring.jpa.hibernate.ddl-auto`            | `update`                                    |
| `spring.cloud.stream.kafka.binder.brokers` | `localhost:9092`                            |
| `spring.cloud.function.definition`         | `paymentPaid`                               |

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
├── src/test/java/dio/ecommerce/checkout
│   └── controller/            # CheckoutControllerTest (JUnit 5 + Testcontainers)
├── pom.xml
└── mvnw / mvnw.cmd
```

---

## 🧠 Decisões técnicas

- **Spring Cloud Stream funcional:** o modelo antigo (`@EnableBinding`, `@Output`, `@Input`, `@StreamListener`) foi removido no Spring Cloud Stream 4.x. A publicação agora usa `StreamBridge` e o consumo usa um bean `Consumer`.
- **Consumidor com `byte[]`:** o bean `paymentPaid` recebe os bytes da mensagem e desserializa o JSON com o `ObjectMapper`. Isso evita falhas de conversão automática de tipo (`ClassCastException: [B cannot be cast to ...`) e funciona independente dos headers da mensagem.
- **JSON no lugar de Avro:** elimina a necessidade de plugin Avro, dependências da Confluent e Schema Registry para este escopo.
- **Binder `kafka` explícito:** o projeto tem também o `spring-cloud-stream-binder-kafka-streams` no classpath, então cada binding declara `binder=kafka` para evitar ambiguidade.
- **Versões alinhadas:** Spring Boot 3.3.x exige Spring Cloud `2023.0.x`. Uma release train mais nova (como `2025.1.x`, feita para o Boot 4) causa `ClassNotFoundException` na subida.
- **Auditoria com Envers:** a entidade `Checkout` é anotada com `@Audited`, mantendo histórico de alterações.
- **`updateStatus` seguro:** só atualiza se o checkout existir e respeita o status recebido (o código original do curso salvava um registro vazio quando o código não era encontrado).
- **JUnit 5 + Testcontainers no lugar de Cucumber:** para um único endpoint, o Cucumber adicionava camadas (arquivo `.feature`, steps e runner) e dependia de JUnit 4. O teste em JUnit 5 cobre o mesmo cenário em um arquivo, com banco e Kafka reais e isolados.

---

## 👤 Autor

Desenvolvido por **Gabriel Oliveira** — [@fxxggah](https://github.com/fxxggah)

Projeto de estudo realizado no desafio prático da [DIO](https://www.dio.me/).
