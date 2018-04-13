DROP TABLE IF  EXISTS tb_FundManagerHistory ;  
CREATE TABLE IF NOT EXISTS  DB_FundsAnalysis.`tb_FundManagerHistory` (
  `id_fund_manager_history` varchar(8) NOT NULL,
  `fund_manager` varchar(255) DEFAULT NULL COMMENT '基金经理',  
  `fund_code` varchar(8) NOT NULL COMMENT '基金代码',  
  `fund_name` varchar(255) NOT NULL COMMENT '基金名称',
  `fund_type` varchar(255) NOT NULL COMMENT '基金类型',    
  `period` varchar(255) DEFAULT NULL COMMENT '任职期间',  
  `start_date` datetime DEFAULT NULL COMMENT '起始期',  
  `end_date` datetime DEFAULT NULL COMMENT '截止期',  
  `return_rate` DECIMAL(8,4) DEFAULT NULL COMMENT '任职回报', 
  `class_average` DECIMAL(8,4) DEFAULT NULL COMMENT '同类平均',
  `class_rank` varchar(255) DEFAULT NULL COMMENT '同类排名',   
   
  PRIMARY KEY (`id_fund_manager_history`)  
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
