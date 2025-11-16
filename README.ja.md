[English](README.md)

# 📈 リアルタイム株価ストリーミングプラットフォーム

このプロジェクトは、**Apache Kafka**、**Apache Flink**、**Spring Boot (SSE API)**、React ダッシュボードを組み合わせた実践的なリアルタイム・データストリーミングプラットフォームです。架空あるいは実際の株価データを取り込み、タイムウィンドウごとに集計し、ダッシュボードへ即時可視化します。

> **備考:**  
> 現在のモックジェネレーターはライブ株価フィードをシミュレートしています。  
> **本番の市場データプロバイダ（取引所・証券会社・REST/WebSocket 等）への差し替えも容易です。**

---

## 🏗️ アーキテクチャ概要

```
┌─────────────────────┐
│ モックジェネレーター    │  • 50銘柄を0.5秒ごとに生成
│ (Spring Boot)       │  • 実データ供給元への置き換え可能
└──────────┬──────────┘
           │
           ▼
┌──────────────────────┐
│   Apache Kafka       │  • KRaftモード（Zookeeper不要）
└──────────┬──────────┘
           │
           ▼
┌──────────────────────┐
│  Flinkプロセッサ       │  • タイムウィンドウ毎に集計（デフォ10秒）
│  (Java/Flink)        │  • 各シンボル毎のavg/min/max/count
└──────────┬──────────┘
           │
           ▼
┌──────────────────────┐
│   Apache Kafka       │  • 集計先トピック："stock-prices-aggregated"
└──────────┬──────────┘
           │
           ▼
┌──────────────────────┐
│  Spring Boot API     │  • 集計取り込み、SSE /api/stocks/stream公開
│  (SSE, Kafka)        │  • 複数クライアント・低レイテンシPUSH
└──────────┬──────────┘
           │
           ▼
┌──────────────────────┐
│  React フロントエンド  │  • リアルタイムダッシュボード
│                      │  • 状態監視、ライブ更新
└──────────────────────┘
```

---

## 🚀 特長 / Features

- **エンドツーエンド低レイテンシ:** データ生成からダッシュボード即時表示まで
- **データソース差し替え可能:** ダミーを本物の市場データへすぐ変更可
- **イベントドリブン・マイクロサービス:** 全サービス常に Kafka 経由非同期
- **Flink 集計:** ウィンドウ単位で銘柄ごとに統計
- **SSE 配信:** 複数前面同時クライアントに低遅延 PUSH
- **ライブ React ダッシュボード:** 自動更新・接続ステータス表示
- **一発起動:** `dev.sh`一発、または各サービス単体運用も可

---

## 🖥️ デモ

<p align="center">
  <img src="./Demo.gif" alt="リアルタイムダッシュボードデモ" width="700"/>
</p>

---

## 📦 技術スタック

### インフラ

- **Apache Kafka 7.6.0**（KRaft モード）
- **Apache Flink 1.19.1**
- **Docker & Docker Compose**

### バックエンド

- **Java 17**（LTS）
- **Spring Boot 3.2.0**
- **Spring Kafka**
- **Maven**

### フロントエンド

- **React 18** + **TypeScript** + **Vite**
- **Recharts**（グラフ描画）

---

## 🏁 クイックスタート

### 前提条件

- [Docker Desktop](https://docs.docker.com/desktop/) または **Docker Engine**（Linux 用）
- **Java 17** ([ダウンロード](https://adoptium.net/))
- **Maven 3.9+**
- **Node.js 18+**

---

### 1️⃣ ワンコマンド一括起動

**開発推薦:**

```bash
git clone https://github.com/drag0sd0g/realtime-stock-price-stream.git
cd realtime-stock-price-stream

# すべて自動ビルド&起動
./dev.sh up
```

- **Flink UI:** http://localhost:8081
- **React ダッシュボード:** http://localhost:5173

**停止:**

```bash
./dev.sh down
```

#### 各コンポーネント個別起動

<details>
<summary>詳細コマンドクリックで展開</summary>

#### Kafka/Flink インフラ起動

```bash
docker-compose up -d
```

#### モック株価ジェネレーター (Spring Boot)

```bash
cd mock-generator
mvn clean install
mvn spring-boot:run
```

#### Flink 処理ジョブ

```bash
cd flink-processor
mvn clean package

# Javaで直接(dev)
mvn exec:java -Dexec.mainClass="com.dragos.stockstream.processor.StockStreamProcessor"

# Flinkクラスタ内部
docker cp target/flink-processor-*.jar flink-jobmanager:/opt/flink/usrlib/
docker exec -it flink-jobmanager flink run /opt/flink/usrlib/flink-processor-*.jar
```

#### バックエンド API (Spring Boot, SSE)

```bash
cd backend-api
mvn clean install
mvn spring-boot:run
# API: http://localhost:8080
```

#### React ダッシュボード

```bash
cd frontend
npm install
npm run dev
# UI: http://localhost:5173
```

</details>

---

### 2️⃣ 動作確認

**生株価は:**

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-prices --from-beginning
```

**集計出力:**

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stock-prices-aggregated --from-beginning
```

**REST/SSE ストリーム:**

```bash
curl http://localhost:8080/api/stocks/stream
```

**ダッシュボード:**  
[http://localhost:5173](http://localhost:5173)  
「Live」表示ならストリーム全体が稼働中。

---

## 📂 プロジェクト構成

```
realtime-stock-price-stream/
├── docker-compose.yml     # インフラ構成
├── dev.sh                 # 一括デブ起動
├── mock-generator/        # (置換OK)株価データ供給
├── flink-processor/       # Flink処理
├── backend-api/           # Spring Boot API (SSE/Kafka)
└── frontend/              # React ダッシュボード
```

---

## 🔧 設定

### Kafka トピック

- **stock-prices:** 生 tick データ
- **stock-prices-aggregated:** ウィンドウ集計済み

### Flink

- **ウィンドウ:** タンブリング（デフォ 10s、変更可）
- **Watermark:** 許容乱れ 5s
- **平行度:** 1（開発用、実運用は調整）

---

## ℹ️ モックジェネレータの実データフィード置換方法

`mock-generator`は Kafka 依存のみの疎結合マイクロサービス。

- **実データ対応:** ここの生成/取得ロジックを、市場データ API・証券会社 API・WebSocket 等に容易に差し替え可能。
- その他のパイプライン（Kafka、Flink、API、UI）は一切変更不要。

---

## 📊 データ例

**モックジェネレータ(`stock-prices`)：**

```json
{
  "symbol": "AAPL",
  "price": 182.45,
  "timestamp": "2025-11-15T14:29:31Z",
  "change": 1.23,
  "changePercent": 0.68
}
```

**Flink 集計出力(`stock-prices-aggregated`)：**

```json
{
  "symbol": "AAPL",
  "avgPrice": 182.34,
  "minPrice": 181.89,
  "maxPrice": 182.67,
  "count": 10,
  "windowStart": "2025-11-15T14:29:30Z",
  "windowEnd": "2025-11-15T14:29:40Z"
}
```

---

## 🛠️ 開発ドキュメント

### 全ビルド

```bash
mvn clean install
```

### モジュール単体テスト

```bash
# 各モジュールディレクトリ(e.g., backend-api)
mvn test
```

### カバレッジレポート

```bash
mvn jacoco:report
```

`target/site/jacoco/index.html`で開く。

### Docker 状態クリーン

```bash
docker-compose down -v
docker system prune -a
```

---

## 🧪 トラブルシューティング

- **Kafka/インフラ起動失敗時:**
  ```bash
  docker-compose down -v
  docker-compose up -d
  docker logs kafka
  ```
- **Flink ジョブ異常時:**  
  `docker logs flink-jobmanager` または [Flink Web UI](http://localhost:8081) 参照
- **UI 無反応:**
  - `/api/stocks/stream` を curl やブラウザで呼出
  - Chrome コンソール&ネットワークでエラー確認
  - バックエンドログでデシリアライズや Kafka エラー確認

---

## 🎯 学べること

- マイクロサービス/イベント駆動アーキテクチャ
- Flink によるストリームウィンドウ処理/集計
- サーバー →Web(ブラウザ) SSE リアルタイム配信
- Docker Compose を用いた開発基盤
- ダミーパイプラインを実質本番データへ即展開

---

## 🤝 コントリビュート

Issue/PR 提案歓迎！  
役立ったらスター ⭐ もぜひお願いします。

---

## 📝 ライセンス

MIT License — [LICENSE](LICENSE)

---

## 👨‍💻 Author

[@drag0sd0g](https://github.com/drag0sd0g)  
☕ & ストリーミング技術愛で開発

---

## 🔗 参考リンク

- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Apache Flink Docs](https://flink.apache.org/docs/stable/)
- [Spring Kafka](https://spring.io/projects/spring-kafka)
- [React](https://react.dev/)
- [SSE (MDN)](https://developer.mozilla.org/ja/docs/Web/API/Server-sent_events)
