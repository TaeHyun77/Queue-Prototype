### 프로젝트 개요
---
이벤트 오픈이나 티켓 예매처럼 짧은 시간에 요청이 몰리는 상황에서 서버의 트래픽을 제어하고, 사용자에게 공정한 대기 순서를 제공하기 위한 대기열 시스템입니다.

사용자는 대기열 등록 후 SSE(Server-Sent Events)를 통해 자신의 순번을 실시간으로 확인하고, 차례가 되면 서비스 접근이 허용됩니다. 대기 순서는 Redis ZSET으로 관리하며, Transactional Outbox와 MySQL Debezium Connector를 통해 상태 변경 이벤트를 Kafka로 전파해 DB와 이벤트 스트림 간의 정합성을 유지했습니다.

이 프로젝트는 이후 다중 서버 환경으로 확장한 Distributed-Queue-Back 프로젝트의 기반이 된 프로젝트로, 핵심 로직과 기술 스택을 단일 서버 환경에서 먼저 검증하기 위해 구현했습니다.<br><br>

**구현 과정**

https://velog.io/@ayeah77/series/%EB%8C%80%EA%B8%B0%EC%97%B4-%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8<br><br>

### 기술  스택
---
Backend : SpringBoot, Spring WebFlux, Spring Data JPA, Java

DB/Cache : MySQL, Redis (Reactive)

Messaging/CDC : Kafka, Kafka Connect, MySQL Debezium Connector

Monitoring : Prometheus, Grafana, Spring Actuator, Micrometer

Infra : Docker, Docker Compose

ETC : SSE, Transactional Outbox Pattern <br><br>

### 아키텍처
---

<p align="center"><img width="1774" height="887" alt="queue-proto-archi" src="https://github.com/user-attachments/assets/92373ada-1b20-4432-b6e8-d6e5cb122ea4" /><br><br>


### 동작 흐름

---

1. 대기열 등록 및 SSE 연결

   클라이언트가 대기열 등록을 요청하면 서버는 해당 클라이언트와 SSE 연결을 맺습니다. 이때 대기열을 구분하는 queueType과 사용자 ID인 userId를 전달하고, 서버는 요청 시각을 timestamp로 기록합니다. 서버는 SSE 스트림을 반환하며, 이후 해당 스트림으로 이벤트가 push될 때마다 클라이언트는 대기열 상태를 실시간으로 전달받을 수 있습니다.
   
2. Redis 대기 순서 관리

   대기열 등록이 완료되면 서버는 Redis의 해당 queueType ZSET에 userId를 key로, 요청 시각을 score로 저장합니다. 이를 통해 요청 시각을 기준으로 사용자의 대기 순서를 자동으로 유지합니다.

3. 상태 변경 및 이벤트 발행

   이후 사용자의 대기열 상태를 DB에 저장하고, 해당 변경 사항을 Debezium Connector가 CDC를 통해 감지합니다. 감지된 변경 이벤트는 Transactional Outbox 패턴을 통해 처리되며, 변경된 대기열의 queueType을 Kafka로 Produce합니다.
   
4. Kafka 이벤트 처리 및 SSE 전송

   Kafka 메시지를 Consumer가 처리하면, 해당 queueType에 연결된 SSE 스트림을 통해 사용자들에게 최신 대기열 상태를 전달합니다. 사용자가 아직 대기 중이라면 현재 순위를 전달하고, 허용열에 진입하면 confirm 이벤트를 전송하여 클라이언트가 서비스 페이지로 이동할 수 있도록 합니다.
   
5. 실시간 대기열 상태 전달

   이와 같은 흐름을 통해 대기열 등록, 삭제, 순번 이동 등 상태가 변경될 때마다 해당 대기열에 연결된 사용자들에게 자신의 대기열 상태를 실시간 SSE 이벤트로 전달합니다.<br><br>

### 구조 개선
---
기존에는 Redis에 대기열 정보를 저장한 뒤 서버에서 직접 SSE 이벤트를 전송했지만, 장애가 발생하여 이벤트가 전달되지 않거나 소실될 경우 서비스에 큰 영향을 줄 수 있었습니다.

이러한 문제를 해결하기 위해 메세지 영속성을 보장하고 재처리가 가능한 Kafka 기반의 이벤트 전송 구조로 개선하였습니다.

또한, 특정 이벤트 발행에 실패할 경우, 데이터베이스에는 사용자의 상태 정보가 갱신되었음에도 이벤트는 전달되지 않아 DB와 이벤트 간의 정합성이 깨질 위험이 있었습니다. 

이를 해결하기 위해 Debezium MySQL Kafka 커넥터를 활용하여 데이터베이스 테이블의 변경 사항을 감지하고 이를 Kafka에 이벤트를 발행하도록 하여 데이터베이스 트랜잭션과 이벤트 간의 정합성 문제를 해결하였습니다.

결과적으로, Debezium 기반의 Transactional Outbox Pattern을 구현함으로써 DB와 이벤트 스트림 간의 불일치 문제를 해결하고, 이벤트 소실 방지, 순서 보장, 데이터 정합성을 해결할 수 있었습니다.<br><br>

### 주요 기술 
---
**Transactional Outbox Pattern과 MySQL Debezium Connector을 사용하는 구조**

<img width="1774" height="887" alt="debezium-archi" src="https://github.com/user-attachments/assets/2beaf663-45b5-4863-802a-2e91a0f3def2" />

[ Transactional Outbox + Debezium 구조 ]

Transactional Outbox 패턴과 Debezium을 함께 사용하면, DB 변경과 이벤트 생성은 하나의 트랜잭션으로 원자성을 보장하고( Outbox ), Outbox 테이블의 이벤트를 메시지 브로커로 전달( Debezium )할 수 있기 때문에 DB 상태와 이벤트 전달 간의 정합성을 맞출 수 있습니다.

Outbox 테이블 변경을 binlog에서 감지하여 Kafka로 publish하고, publish 실패 시 Debezium이 자동으로 재시도하므로 이벤트 유실이 없습니다.

이를 통해 DB 상태와 Kafka 이벤트 스트림 사이의 정합성을 유지할 수 있습니다.<br><br>

### 추후 개선점
---
1. 비동기 로직에서 동기적인 DB 사용은 심각한 병목이 될 수 있으며, 대기열 시스템의 특성 상 사용자의 상태 정보를 저장할 필요가 없으므로 DB를 제거하고자 합니다.

   또한, 멱등성 정보를 저장할 때는 R2DBC를 활용하여 전체 비동기 구조와 일관되게 처리할 수 있도록 하고자 합니다.

2. Kotlin 기반으로 변경하여 코루틴을 사용함으로써 WebFlux의 Reactor 체인 구조를 동기적인 형식의 코드로 개선하여 가독성과 유지 보수성을 높히고자 합니다.

3. 단일 서버에서 분산 서버로 확장 함으로써 부하 분산 및 가용성을 확보할 계획입니다.
