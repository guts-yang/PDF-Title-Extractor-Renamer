#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
论文机构信息报告生成脚本
用于从现有PDF文件结构中提取以下信息并生成表格：
- 论文所属机构（文件夹名称）
- 细分子机构（论文出版社）
- 论文标题
"""

import os
import re
import pandas as pd
from datetime import datetime
import logging

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = os.path.join(log_dir, f"paper_institution_report_{timestamp}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class PaperInstitutionReporter:
    """论文机构信息报告生成器"""
    
    def __init__(self):
        """初始化报告生成器"""
        self.root_dir = os.path.dirname(os.path.abspath(__file__))
        self.output_excel = os.path.join(log_dir, f"论文机构信息汇总_{timestamp}.xlsx")
        self.report_file = os.path.join(log_dir, f"机构分布报告_{timestamp}.txt")
        
        # 主要机构文件夹映射
        self.institution_mapping = {
            "AAAI": "AAAI",
            "ACM": "ACM",
            "EBSCO": "EBSCO",
            "EI": "EI",
            "ICML": "ICML",
            "Nature": "Nature",
            "ScienceDirect": "ScienceDirect",
            "知网": "中国知网"
        }
        
        # 出版社识别关键词
        self.publisher_patterns = {
            "AAAI Press": ["AAAI"],
            "ACM": ["ACM"],
            "EBSCO Publishing": ["EBSCO"],
            "Elsevier": ["ScienceDirect", "Elsevier"],
            "Engineering Index": ["EI"],
            "ICML": ["ICML"],
            "Nature Publishing Group": ["Nature"],
            "中国知网": ["知网", "CNKI"]
        }
        
        # 数据存储
        self.papers_data = []
        
        # 统计信息
        self.stats = {
            "total_papers": 0,
            "institution_counts": {},
            "publisher_counts": {}
        }
    
    def sanitize_filename(self, filename):
        """清理文件名，移除.pdf扩展名和可能的序号"""
        # 移除.pdf扩展名
        if filename.lower().endswith('.pdf'):
            filename = filename[:-4]
        
        # 移除末尾的序号（如 _1, -1）
        filename = re.sub(r'[\-_]\d+$', '', filename)
        
        return filename.strip()
    
    def identify_publisher(self, institution, filename):
        """根据机构和文件名识别出版社"""
        text_to_check = f"{institution} {filename}".lower()
        
        # 优先根据机构匹配出版社
        for publisher, keywords in self.publisher_patterns.items():
            for keyword in keywords:
                if keyword.lower() == institution.lower():
                    return publisher
        
        # 根据文件名内容匹配
        for publisher, keywords in self.publisher_patterns.items():
            for keyword in keywords:
                if keyword.lower() in text_to_check:
                    return publisher
        
        # 如果无法识别，返回默认值
        if institution in self.institution_mapping:
            return self.institution_mapping[institution]
        
        return "未知出版社"
    
    def extract_title_from_filename(self, filename):
        """从文件名中提取论文标题"""
        # 清理文件名
        title = self.sanitize_filename(filename)
        
        # 尝试移除可能的作者信息（通常在文件名开头或末尾）
        # 如 "基于布隆过滤器的联邦遗忘学习_陈萍" -> "基于布隆过滤器的联邦遗忘学习"
        title = re.sub(r'[_-][\u4e00-\u9fa5]+$', '', title)
        
        # 尝试移除可能的年份信息
        title = re.sub(r'[_-]\d{4}$', '', title)
        
        return title.strip()
    
    def scan_directories(self):
        """扫描目录结构，提取论文信息"""
        logger.info(f"开始扫描目录: {self.root_dir}")
        
        # 遍历主要机构文件夹
        for folder_name in os.listdir(self.root_dir):
            folder_path = os.path.join(self.root_dir, folder_name)
            
            # 跳过非目录和系统文件夹
            if not os.path.isdir(folder_path) or folder_name.startswith('.') or folder_name in ['logs']:
                continue
            
            # 检查是否是已知机构
            if folder_name not in self.institution_mapping:
                logger.warning(f"未知机构文件夹: {folder_name}")
                institution = folder_name
            else:
                institution = self.institution_mapping[folder_name]
            
            logger.info(f"处理机构: {institution} ({folder_name})")
            
            # 扫描该机构文件夹下的所有PDF文件
            self._scan_folder(folder_path, institution, folder_name)
        
        logger.info(f"扫描完成，共发现 {self.stats['total_papers']} 篇论文")
    
    def _scan_folder(self, folder_path, institution, original_folder):
        """递归扫描文件夹"""
        try:
            for item in os.listdir(folder_path):
                item_path = os.path.join(folder_path, item)
                
                # 如果是子文件夹，递归处理
                if os.path.isdir(item_path):
                    # 子文件夹可能表示更细分的机构
                    sub_institution = f"{institution} - {item}"
                    logger.info(f"进入子文件夹: {item}")
                    self._scan_folder(item_path, sub_institution, original_folder)
                
                # 处理PDF文件
                elif item.lower().endswith('.pdf'):
                    try:
                        # 提取标题
                        title = self.extract_title_from_filename(item)
                        
                        # 识别出版社
                        publisher = self.identify_publisher(original_folder, item)
                        
                        # 保存数据
                        self.papers_data.append({
                            "论文所属机构": institution,
                            "细分子机构": publisher,
                            "论文标题": title,
                            "原始文件名": item,
                            "文件路径": folder_path
                        })
                        
                        # 更新统计信息
                        self.stats["total_papers"] += 1
                        self.stats["institution_counts"][institution] = self.stats["institution_counts"].get(institution, 0) + 1
                        self.stats["publisher_counts"][publisher] = self.stats["publisher_counts"].get(publisher, 0) + 1
                        
                        # 进度提示
                        if self.stats["total_papers"] % 10 == 0:
                            logger.info(f"已处理 {self.stats['total_papers']} 篇论文")
                            
                    except Exception as e:
                        logger.error(f"处理文件 {item} 时出错: {str(e)}")
                        
        except Exception as e:
            logger.error(f"扫描文件夹 {folder_path} 时出错: {str(e)}")
    
    def generate_excel_report(self):
        """生成Excel报告"""
        if not self.papers_data:
            logger.error("没有论文数据可生成报告")
            return False
        
        try:
            # 创建DataFrame
            df = pd.DataFrame(self.papers_data)
            
            # 只保留需要的三列
            report_df = df[["论文所属机构", "细分子机构", "论文标题"]]
            
            # 保存为Excel文件
            with pd.ExcelWriter(self.output_excel, engine='openpyxl') as writer:
                report_df.to_excel(writer, index=False, sheet_name='论文机构信息')
                
                # 格式化工作表
                worksheet = writer.sheets['论文机构信息']
                
                # 设置列宽
                column_widths = {
                    '论文所属机构': 30,
                    '细分子机构': 30,
                    '论文标题': 60
                }
                
                # 应用列宽设置
                for column in worksheet.columns:
                    column_letter = column[0].column_letter
                    column_name = column[0].value
                    
                    if column_name in column_widths:
                        worksheet.column_dimensions[column_letter].width = column_widths[column_name]
                    else:
                        # 自动调整宽度
                        max_length = 0
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                # 设置标题行字体加粗
                for cell in worksheet[1]:  # 标题行
                    cell.font = cell.font.copy(bold=True)
            
            logger.info(f"Excel报告已生成: {self.output_excel}")
            return True
            
        except Exception as e:
            logger.error(f"生成Excel报告失败: {str(e)}")
            return False
    
    def generate_text_report(self):
        """生成文本格式的统计报告"""
        try:
            with open(self.report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("论文机构分布统计报告\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                f.write("1. 总体统计\n")
                f.write("-" * 40 + "\n")
                f.write(f"论文总数: {self.stats['total_papers']}\n")
                f.write(f"机构数量: {len(self.stats['institution_counts'])}\n")
                f.write(f"出版社数量: {len(self.stats['publisher_counts'])}\n\n")
                
                f.write("2. 机构分布统计\n")
                f.write("-" * 40 + "\n")
                for institution, count in sorted(self.stats['institution_counts'].items(), key=lambda x: x[1], reverse=True):
                    percentage = count / self.stats['total_papers'] * 100
                    f.write(f"{institution}: {count} 篇 ({percentage:.2f}%)\n")
                f.write("\n")
                
                f.write("3. 出版社分布统计\n")
                f.write("-" * 40 + "\n")
                for publisher, count in sorted(self.stats['publisher_counts'].items(), key=lambda x: x[1], reverse=True):
                    percentage = count / self.stats['total_papers'] * 100
                    f.write(f"{publisher}: {count} 篇 ({percentage:.2f}%)\n")
                f.write("\n")
                
                f.write("4. 输出文件信息\n")
                f.write("-" * 40 + "\n")
                f.write(f"Excel报告文件: {self.output_excel}\n")
                f.write(f"统计报告文件: {self.report_file}\n")
                f.write(f"日志文件: {log_file}\n")
            
            logger.info(f"统计报告已生成: {self.report_file}")
            return True
            
        except Exception as e:
            logger.error(f"生成统计报告失败: {str(e)}")
            return False
    
    def run(self):
        """运行整个报告生成流程"""
        logger.info("开始生成论文机构信息报告...")
        
        # 步骤1: 扫描目录，提取论文信息
        self.scan_directories()
        
        if not self.papers_data:
            logger.error("没有找到论文文件，程序终止")
            return False
        
        # 步骤2: 生成Excel报告
        if not self.generate_excel_report():
            logger.error("生成Excel报告失败")
        
        # 步骤3: 生成统计报告
        if not self.generate_text_report():
            logger.error("生成统计报告失败")
        
        logger.info("论文机构信息报告生成完成！")
        logger.info(f"共处理 {self.stats['total_papers']} 篇论文")
        logger.info(f"Excel报告: {self.output_excel}")
        logger.info(f"统计报告: {self.report_file}")
        
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
    
    # 创建报告生成器并运行
    reporter = PaperInstitutionReporter()
    success = reporter.run()
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)