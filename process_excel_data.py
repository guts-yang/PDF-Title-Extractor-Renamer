#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
表格数据处理脚本
用于处理论文数据表格，提取并规范化以下信息：
- 论文所属机构（文件夹名称）
- 细分子机构（论文出版社）
- 论文标题
"""

import os
import pandas as pd
import re
import json
from datetime import datetime
import logging

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"excel_processor_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class ExcelProcessor:
    """Excel数据处理类"""
    
    def __init__(self):
        """初始化处理器"""
        self.input_file = os.path.join(os.path.dirname(__file__), "EI", "CPXSourceList_102025.xlsx")
        self.output_excel = os.path.join(log_dir, f"processed_papers_{timestamp}.xlsx")
        self.output_json = os.path.join(log_dir, f"processed_papers_{timestamp}.json")
        self.report_file = os.path.join(log_dir, f"processing_report_{timestamp}.txt")
        
        # 定义文件夹名称映射（根据现有项目结构）
        self.institution_folders = {
            "AAAI": ["AAAI", "AAAI Press"],
            "ACM": ["ACM", "Association for Computing Machinery"],
            "EI": ["EI", "Engineering Index"],
            "ICML": ["ICML", "International Conference on Machine Learning"],
            "Nature": ["Nature", "Nature Publishing Group"],
            "ScienceDirect": ["ScienceDirect", "Elsevier"],
            "知网": ["CNKI", "中国知网", "知网"]
        }
        
        # 数据统计信息
        self.stats = {
            "total_rows": 0,
            "processed_rows": 0,
            "invalid_rows": 0,
            "institution_counts": {},
            "publisher_counts": {},
            "errors": []
        }
    
    def sanitize_text(self, text):
        """清理和规范化文本数据"""
        if pd.isna(text):
            return ""
        
        # 转换为字符串并去除前后空格
        text = str(text).strip()
        
        # 处理特殊字符和多余的空白
        text = re.sub(r'\s+', ' ', text)
        text = text.replace('\t', ' ').replace('\n', ' ')
        
        # 去除控制字符
        text = ''.join(char for char in text if ord(char) >= 32 or char == '\t' or char == '\n')
        
        return text
    
    def map_institution_to_folder(self, institution_name):
        """将机构名称映射到文件夹名称"""
        institution_name = institution_name.lower()
        
        for folder, keywords in self.institution_folders.items():
            for keyword in keywords:
                if keyword.lower() in institution_name:
                    return folder
        
        # 默认返回未知机构
        return "未知机构"
    
    def extract_publisher(self, text):
        """从文本中提取出版社信息"""
        # 简单的出版社提取逻辑，可以根据实际数据调整
        publishers = [
            "Elsevier", "Springer", "IEEE", "ACM", "AAAI Press", 
            "Nature Publishing Group", "ScienceDirect", "Taylor & Francis",
            "Wiley", "Oxford University Press", "Cambridge University Press",
            "中国知网", "CNKI", "EI", "Engineering Index"
        ]
        
        text_lower = text.lower()
        for publisher in publishers:
            if publisher.lower() in text_lower:
                return publisher
        
        return "未知出版社"
    
    def load_excel_data(self):
        """加载Excel数据"""
        try:
            logger.info(f"开始加载Excel文件: {self.input_file}")
            # 使用pandas读取Excel文件
            df = pd.read_excel(self.input_file)
            logger.info(f"Excel文件加载成功，共包含 {len(df)} 行数据")
            self.stats["total_rows"] = len(df)
            return df
        except Exception as e:
            logger.error(f"加载Excel文件失败: {str(e)}")
            self.stats["errors"].append(f"加载Excel文件失败: {str(e)}")
            return None
    
    def process_data(self, df):
        """处理数据"""
        if df is None or df.empty:
            return None
        
        logger.info("开始处理数据...")
        
        # 查看前几行数据以了解结构
        logger.info("\n数据列信息:")
        for col in df.columns:
            logger.info(f"- {col}")
        
        logger.info("\n前5行数据样本:")
        logger.info(str(df.head()))
        
        # 创建结果DataFrame
        result_df = pd.DataFrame(columns=["论文所属机构", "细分子机构", "论文标题"])
        
        # 尝试识别正确的列
        # 通常标题列可能包含"title"、"标题"等关键词
        # 机构和出版社信息可能在其他列中
        title_column = None
        institution_column = None
        publisher_column = None
        
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ["title", "标题", "题目"]):
                title_column = col
            elif any(keyword in col_lower for keyword in ["institution", "机构", "组织"]):
                institution_column = col
            elif any(keyword in col_lower for keyword in ["publisher", "出版社", "出版", "journal", "期刊"]):
                publisher_column = col
        
        logger.info(f"\n识别到的列映射:")
        logger.info(f"标题列: {title_column}")
        logger.info(f"机构列: {institution_column}")
        logger.info(f"出版社列: {publisher_column}")
        
        # 处理每一行数据
        for index, row in df.iterrows():
            try:
                # 提取标题
                title = ""
                if title_column and title_column in row:
                    title = self.sanitize_text(row[title_column])
                else:
                    # 如果没有明确的标题列，尝试从其他列中提取
                    for col in df.columns:
                        if col not in [institution_column, publisher_column]:
                            potential_title = self.sanitize_text(row[col])
                            if len(potential_title) > len(title):
                                title = potential_title
                
                # 提取机构信息
                institution_text = ""
                if institution_column and institution_column in row:
                    institution_text = self.sanitize_text(row[institution_column])
                elif publisher_column and publisher_column in row:
                    # 如果没有机构列，使用出版社列作为补充
                    institution_text = self.sanitize_text(row[publisher_column])
                else:
                    # 使用整行信息尝试提取
                    row_text = " ".join([str(val) for val in row.values if pd.notna(val)])
                    institution_text = row_text
                
                # 映射到文件夹名称
                folder_name = self.map_institution_to_folder(institution_text)
                
                # 提取出版社信息
                publisher_text = ""
                if publisher_column and publisher_column in row:
                    publisher_text = self.sanitize_text(row[publisher_column])
                else:
                    # 从机构文本或整行信息中提取
                    if institution_text:
                        publisher_text = self.extract_publisher(institution_text)
                    else:
                        row_text = " ".join([str(val) for val in row.values if pd.notna(val)])
                        publisher_text = self.extract_publisher(row_text)
                
                # 验证数据有效性
                if not title:
                    self.stats["invalid_rows"] += 1
                    self.stats["errors"].append(f"第{index+1}行: 缺少标题信息")
                    continue
                
                # 添加到结果
                result_df.loc[len(result_df)] = [folder_name, publisher_text, title]
                
                # 更新统计信息
                self.stats["processed_rows"] += 1
                self.stats["institution_counts"][folder_name] = self.stats["institution_counts"].get(folder_name, 0) + 1
                self.stats["publisher_counts"][publisher_text] = self.stats["publisher_counts"].get(publisher_text, 0) + 1
                
                # 进度提示
                if (index + 1) % 1000 == 0:
                    logger.info(f"已处理 {index + 1} 行数据")
                    
            except Exception as e:
                self.stats["invalid_rows"] += 1
                self.stats["errors"].append(f"第{index+1}行处理失败: {str(e)}")
                logger.error(f"处理第{index+1}行时出错: {str(e)}")
        
        logger.info(f"数据处理完成，成功处理 {self.stats['processed_rows']} 行，无效数据 {self.stats['invalid_rows']} 行")
        return result_df
    
    def save_results(self, result_df):
        """保存处理结果"""
        if result_df is None or result_df.empty:
            logger.error("没有数据可保存")
            return False
        
        try:
            # 保存为Excel文件
            result_df.to_excel(self.output_excel, index=False, engine='openpyxl')
            logger.info(f"处理结果已保存到: {self.output_excel}")
            
            # 保存为JSON文件
            result_dict = result_df.to_dict(orient='records')
            with open(self.output_json, 'w', encoding='utf-8') as f:
                json.dump(result_dict, f, ensure_ascii=False, indent=2)
            logger.info(f"处理结果已保存到: {self.output_json}")
            
            return True
        except Exception as e:
            logger.error(f"保存结果失败: {str(e)}")
            self.stats["errors"].append(f"保存结果失败: {str(e)}")
            return False
    
    def generate_report(self):
        """生成处理报告"""
        try:
            with open(self.report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("论文数据处理报告\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"处理时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("1. 数据处理统计\n")
                f.write("-" * 40 + "\n")
                f.write(f"总数据行数: {self.stats['total_rows']}\n")
                f.write(f"成功处理行数: {self.stats['processed_rows']}\n")
                f.write(f"无效数据行数: {self.stats['invalid_rows']}\n")
                f.write(f"处理成功率: {self.stats['processed_rows']/self.stats['total_rows']*100:.2f}%\n\n")
                
                f.write("2. 机构分布统计\n")
                f.write("-" * 40 + "\n")
                for institution, count in sorted(self.stats['institution_counts'].items(), key=lambda x: x[1], reverse=True):
                    f.write(f"{institution}: {count} 篇 ({count/self.stats['processed_rows']*100:.2f}%)\n")
                f.write("\n")
                
                f.write("3. 出版社分布统计（前20名）\n")
                f.write("-" * 40 + "\n")
                top_publishers = sorted(self.stats['publisher_counts'].items(), key=lambda x: x[1], reverse=True)[:20]
                for publisher, count in top_publishers:
                    f.write(f"{publisher}: {count} 篇\n")
                f.write("\n")
                
                if self.stats['errors']:
                    f.write("4. 错误记录\n")
                    f.write("-" * 40 + "\n")
                    for error in self.stats['errors'][:50]:  # 只记录前50条错误
                        f.write(f"- {error}\n")
                    if len(self.stats['errors']) > 50:
                        f.write(f"... 还有 {len(self.stats['errors']) - 50} 条错误未显示\n")
                    f.write("\n")
                
                f.write("5. 输出文件信息\n")
                f.write("-" * 40 + "\n")
                f.write(f"Excel结果文件: {self.output_excel}\n")
                f.write(f"JSON结果文件: {self.output_json}\n")
                f.write(f"日志文件: {log_file}\n")
                
            logger.info(f"处理报告已生成: {self.report_file}")
            return True
        except Exception as e:
            logger.error(f"生成报告失败: {str(e)}")
            return False
    
    def run(self):
        """运行整个处理流程"""
        logger.info("开始处理论文数据表格...")
        
        # 步骤1: 加载数据
        df = self.load_excel_data()
        if df is None:
            logger.error("数据加载失败，程序终止")
            return False
        
        # 步骤2: 处理数据
        result_df = self.process_data(df)
        if result_df is None or result_df.empty:
            logger.error("数据处理失败，没有有效数据")
            return False
        
        # 步骤3: 保存结果
        if not self.save_results(result_df):
            logger.error("保存结果失败")
            return False
        
        # 步骤4: 生成报告
        if not self.generate_report():
            logger.error("生成报告失败")
        
        logger.info("论文数据表格处理完成！")
        return True

def main():
    """主函数"""
    # 检查必要的依赖库
    try:
        import pandas as pd
        import openpyxl
    except ImportError:
        print("错误: 缺少必要的依赖库")
        print("请运行: pip install pandas openpyxl")
        return 1
    
    # 创建处理器并运行
    processor = ExcelProcessor()
    success = processor.run()
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)