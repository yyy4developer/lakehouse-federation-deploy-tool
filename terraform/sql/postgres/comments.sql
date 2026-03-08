-- テーブルコメント
COMMENT ON TABLE machines IS '機械マスタ - 工場内全機械の製造ライン配置と稼働状態を管理';
COMMENT ON TABLE maintenance_logs IS '保守ログ - 各機械の保守作業履歴を記録';
COMMENT ON TABLE work_orders IS '作業指示書 - 保守作業の計画と進捗を管理';

-- machines カラムコメント
COMMENT ON COLUMN machines.machine_id IS '機械の一意識別子';
COMMENT ON COLUMN machines.machine_name IS '機械の表示名（例：CNCミル01）';
COMMENT ON COLUMN machines.production_line IS '製造ライン配置（ライン1、ライン2、ライン3）';
COMMENT ON COLUMN machines.factory IS '工場建屋の所在地（A棟、B棟、C棟）';
COMMENT ON COLUMN machines.status IS '現在の稼働状態：稼働中、メンテナンス中、停止中';

-- maintenance_logs カラムコメント
COMMENT ON COLUMN maintenance_logs.log_id IS '保守ログID';
COMMENT ON COLUMN maintenance_logs.machine_id IS '対象機械ID (1-10)';
COMMENT ON COLUMN maintenance_logs.log_date IS '作業実施日';
COMMENT ON COLUMN maintenance_logs.action IS '実施した保守作業の内容';
COMMENT ON COLUMN maintenance_logs.technician IS '担当技術者名';
COMMENT ON COLUMN maintenance_logs.duration_min IS '作業所要時間（分）';
COMMENT ON COLUMN maintenance_logs.notes IS '備考・詳細メモ';

-- work_orders カラムコメント
COMMENT ON COLUMN work_orders.order_id IS '作業指示書ID';
COMMENT ON COLUMN work_orders.machine_id IS '対象機械ID (1-10)';
COMMENT ON COLUMN work_orders.priority IS '優先度 (CRITICAL/HIGH/MEDIUM/LOW)';
COMMENT ON COLUMN work_orders.status IS 'ステータス (open/in_progress/completed)';
COMMENT ON COLUMN work_orders.created_date IS '作成日';
COMMENT ON COLUMN work_orders.due_date IS '期限日';
COMMENT ON COLUMN work_orders.assigned_to IS '担当者名';
