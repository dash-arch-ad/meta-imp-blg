
## 注意事項

### Google Ads 認証の注意

* MCC 配下のみで動作する。外部アカウントは、サブアカウントにする必要がある
* MCC 経由には「アカウント→サブアカウント→+マークで既存のアカウントをリンク→サブアカウント側の管理者でリンク承認→MCC配下になる」
* OAuth の `refresh_token` を発行した Google ユーザーの権限でアクセス可否が決まる

---

### 媒体ごとの指標差分について
本スクリプトは、Meta / TikTok / Google のユニークリーチ系指標を 1 つのシートにまとめて出力します。  
ただし、媒体ごとに API 仕様と指標の定義が完全には一致しないため、同じ列名でも厳密には同義ではありません。

- **Meta**
  - `unique_reach` は `reach`
  - `unique_ad_recall_lift` は `estimated_ad_recallers`
- **TikTok**
  - `unique_reach` は `reach`
  - `unique_ad_recall_lift` は未使用
- **Google**
  - `unique_reach` は `metrics.unique_users`
  - `unique_ad_recall_lift` は未使用

### Google Ads の注意
Google Ads は Meta / TikTok と同じ粒度では取得できません。

- `campaign` と `campaign_day` は正式な明細値として取得
- `all` と `day` は、`campaign` クエリの **summary row** を採用
- `adset` と `ad` は Google Ads public API の制約により未対応

そのため、Google の `all` / `day` は  
**「広告アカウント全体の customer 直取得値」ではなく、campaign ベースの合計行に相当する値**  
として扱っています。

### Google Ads の指標仕様
Google の `metrics.unique_users` には以下の制約があります。

- 集計不可の指標
- 92日以内の期間でのみ取得可能
- 主に Display / Video / Discovery / App キャンペーンで利用可能
- Meta / TikTok の account-level reach と厳密に同義ではない

このため、媒体横断で数値を比較する際は、  
**Google だけ概念と取得方法が少し異なる** 前提で参照してください。

---

## 実装上の補足

* Google の `all` / `day` は暫定実装です
* 将来的に Google Ads API 側の仕様変更や UI 集計との差異が出る可能性があります
* 媒体横断で完全に同じ意味の reach を比較しているわけではありません
