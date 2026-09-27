# 节日商圈结算协同

本项目保存节日商圈结算协同所需的领域上下文和校验契约，便于服务端功能围绕真实业务参与方展开。当前版本只提供资料读取、结构校验和命令行摘要，数据均为演示用虚构内容。

## 参与方

商务局专员、商圈运营方、参加活动的商户、财务审计人员

## 事实资料

- 北京中秋假期127家重点监测企业实现销售40.4亿元
- 70个重点商圈客流达2708.8万人次
- 文化娱乐和旅游售票等消费类别出现不同幅度增长

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 编译

```bash
python3 -m compileall -q src tests
```

## 命令行检查

```bash
python3 -m src.holiday_settlement.context fixtures/context.json
```
