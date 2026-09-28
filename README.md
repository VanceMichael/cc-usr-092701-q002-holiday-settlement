# 节日商圈结算协同

面向商圈运营的结算协同服务：接收企业、活动、券批次、核销、退款、支付对账记录，
按活动规则冻结可用额度，并在结算窗口生成可解释的分账结果。服务采用事件溯源
（命令 → 事件 → 哈希链日志 → 状态投影），重启后重放日志即可还原全部账务。

## 参与方与职责边界

| 角色 | 常量 | 能做什么 |
|---|---|---|
| 商圈运营人员 | `OPERATOR` | 登记企业、接收核销/退款/支付/客流销售记录、开窗、发起争议 |
| 财务人员 | `FINANCE` | 存预算、生成窗口分账、封账（只处理金额） |
| 活动主办方 | `ORGANIZER` | 定义活动规则（比例/封顶）、券批次 |
| 审计角色 | `AUDITOR` | 唯一能解锁争议单的角色（放行/维持/改派） |

## 核心账务规则

- **额度恒等式**，任何时刻（含重启、乱序回执后）都成立：
  `预算总额 = 可用 + 冻结 + 已结算`；`reconcile()` 还会把池余额与订单
  汇总逐项勾稽（冻结额=冻结态订单补贴之和，已结算=已结算态订单之和）。
- **核销即冻结**：券码核销按当时的活动规则版本（比例/单笔封顶/券面值/活动上限）
  计算补贴并冻结；银联支付到达后才允许进入分账，支付未匹配的订单额度保持冻结。
- **共享年度预算**：多个活动（品牌快闪、夜游、餐饮节）可指向同一个预算池，
  任一活动的冻结都会减少全池可用额度。
- **禁止二次补贴**：一个银联支付引用在任一活动领过补贴后，其他活动再引用即
  抛 `RuleConflictError`；一张券码全局只能核销一次。
- **渠道重传隔离**：按 `(来源, 渠道流水号) + 内容指纹` 识别。同内容重传幂等
  吸收；**金额相同但订单内容不同**的重传进隔离区（`retransmit_diff`），
  不冻结任何额度；同一幂等键换内容提交则直接拒绝。
- **封账快照不可变**：封账（`close_books`）定格 `SNAP-<窗口>`。封账后商户补报
  同窗口记录进隔离区（`late_after_close`），绝不回写快照。
- **退款三种时点**：
  - 结算前退款：冻结释放回可用；
  - 已结算未封账：直接更正本期结算单（移除分录、修正企业/活动汇总），封账定格即最终数；
  - 封账后退款：快照不变，补贴回池并挂「下期追减」调整单，在下一窗口 `applied`。
  - 跨店退款记录退款发起方与**承担返还的企业**（实际领取补贴的企业）。
- **乱序回执**：支付先于核销、退款先于订单到达均可；退款先到时挂起，
  订单补齐并冻结后自动释放，不会重复占额。
- **争议单**：争议订单被排除出分账且冻结不动、退款被拒绝；只有审计能解锁。
- **可追溯**：每张结算单的每个汇总数字都能钻取到单笔分录，分录带
  规则版本事件号、核销事件号、银联支付事件号与文字解释；订单时间线覆盖每次调整。
- **防篡改**：日志为 SHA-256 哈希链，加载时校验断链/摘要/序号，篡改即报错。

## 代码结构

```
src/holiday_settlement/
  errors.py     领域异常（额度不足/封账/争议/重传冲突/权限等）
  auth.py       四种角色与命令权限映射
  records.py    金额换算（分）、Command/Event、事件类型
  journal.py    哈希链 JSONL 日志（原子落盘、重启重放、完整性校验）
  state.py      结算状态机：命令校验 + 事件投影 + 对账 + 追溯
  service.py    SettlementService 门面：鉴权→校验→追加→投影；批量 ingest
examples/demo.py             端到端业务叙事演示
tests/test_settlement.py     32 个端到端测试
contracts/context.schema.json fixtures/context.json  领域资料（原有）
```

## 快速上手

```python
from src.holiday_settlement import SettlementService, Principal, OPERATOR, FINANCE, ORGANIZER, AUDITOR, to_cents

svc = SettlementService("data/journal.jsonl")  # 传路径即持久化；不传为纯内存
op, fin, org, aud = (
    Principal("小林", OPERATOR), Principal("周姐", FINANCE),
    Principal("老陈", ORGANIZER), Principal("赵工", AUDITOR),
)

svc.execute("register_company", "c1", {"company_id": "C1", "name": "烤鸭店"}, op)
svc.execute("deposit_budget", "d1", {"pool_id": "Y2026", "amount_cents": to_cents("500000")}, fin)
svc.execute("define_activity", "a1",
            {"activity_id": "POPUP", "name": "品牌快闪", "pool_id": "Y2026",
             "ratio_permille": 1000, "per_order_cap_cents": 2000}, org)
svc.execute("define_coupon_batch", "b1", {"batch_id": "B1", "activity_id": "POPUP"}, org)
svc.execute("open_window", "w1", {"period_id": "P1", "activity_ids": ["POPUP"]}, op)
svc.execute("receive_redemption", "r1",
            {"order_id": "O1", "company_id": "C1", "batch_id": "B1", "code": "CP1",
             "gross_cents": 26800, "period_id": "P1",
             "source": "brand", "channel_ref": "BR1"}, op)
svc.execute("receive_payment", "u1",
            {"order_id": "O1", "company_id": "C1", "payment_reference": "UP1",
             "amount_cents": 26800, "period_id": "P1",
             "source": "unionpay", "channel_ref": "UP1"}, op)
svc.execute("settle_window", "s1", {"period_id": "P1"}, fin)
svc.execute("close_books", "x1", {"period_id": "P1"}, fin)

svc.reconcile()                 # {'ok': True, ...}
svc.trace_settlement("ST-P1")   # 汇总 → 分录 → 核销/支付原始事件号
```

分批到达的数据可用 `svc.ingest(commands, principal)` 按序排空，单条失败/隔离
不影响其余记录，结果清单逐项标注。

## 测试与演示

```bash
python3 -m unittest discover -s tests -v     # 32 个测试
python3 -m compileall -q src tests examples
python3 -m examples.demo                     # 端到端业务叙事
python3 -m src.holiday_settlement.context fixtures/context.json
```

所有金额在领域内部以整数「分」表示，命令入口可用 `to_cents("268")` 换算，
展示用 `to_yuan(cents)`。数据均为演示用虚构内容。
