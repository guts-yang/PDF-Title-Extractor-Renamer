import os
import re
import logging
from datetime import datetime
import PyPDF2
import pdfplumber
import shutil
from collections import defaultdict
import pandas as pd
import argparse
import traceback

# 配置日志
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'pdf_rename_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 配置参数
class Config:
    # 非法字符替换映射
    ILLEGAL_CHARS = {'<': '(', '>': ')', ':': ' -', '"': '', '/': '-', '\\': '-', '|': '-', '?': '', '*': ''}
    
    # PDF处理配置
    MAX_TITLE_LENGTH = 150
    MAX_PAGES_TO_CHECK = 3
    MIN_TITLE_LENGTH = 5
    
    # 内容提取区域配置
    HEADER_REGION = (0.0, 0.0, 1.0, 0.15)  # 页眉区域 (左, 下, 右, 上)
    CONTENT_REGION = (0.1, 0.1, 0.9, 0.8)  # 正文区域
    FOOTER_REGION = (0.0, 0.8, 1.0, 1.0)  # 页脚区域
    
    # 关键词配置 - 用于标题验证和机构提取
    TITLE_KEYWORDS = [
        'research', 'study', 'investigation', 'analysis', 'method', 'approach',
        'algorithm', 'model', 'system', 'framework', 'technique', 'solution',
        'theory', 'design', 'implementation', 'evaluation', 'comparison'
    ]
    
    INSTITUTION_KEYWORDS = [
        'university', 'institute', 'lab', 'laboratory', 'school', 'college',
        'department', 'center', 'faculty', 'academy', 'research', 'institute of',
        'university of', 'dept.', 'school of', 'college of'
    ]
    
    # 文件名格式配置
    DEFAULT_NAME_FORMAT = "未识别文件_{timestamp}"
    
    # 论文标准命名格式 - 仅包含标题格式
    PAPER_NAMING_FORMAT = "{title}"
    
    # 年份模式（用于从文件名或文本中提取年份）
    YEAR_PATTERNS = [
        r'\b(19|20)\d{2}\b',  # 19xx或20xx格式的年份
        r'\((19|20)\d{2}\)',  # (19xx)或(20xx)格式的年份
    ]
    
    # 作者模式（用于从文件名或文本中提取作者）
    AUTHOR_PATTERNS = [
        r'([A-Z][a-z]+)\s+et\s+al',  # 姓 et al 格式
        r'([A-Z][a-z]+(?:,\s*[A-Z][a-z]+)*)',  # 姓1, 姓2 格式
    ]
    
    # 关键词提取配置
    MAX_KEYWORDS_LENGTH = 30
    KEYWORD_MIN_LENGTH = 3
    
    # 跳过的文件模式
    SKIP_PATTERNS = [
        r'^\.',  # 隐藏文件
        r'^~\$',  # 临时文件
        r'^Thumbs\.db$',  # Windows缩略图缓存
        r'^desktop\.ini$'  # Windows配置文件
    ]


# 全局变量初始化
log_dir = None
log_file = None
logger = None

def sanitize_filename(filename):
    """清理文件名中的非法字符"""
    if not filename:
        return "未命名"
    
    # 替换非法字符
    for char, replacement in Config.ILLEGAL_CHARS.items():
        filename = filename.replace(char, replacement)
    
    # 移除多余的空格和点号
    filename = re.sub(r'\s+', ' ', filename).strip()
    filename = re.sub(r'\s*\.\s*', '.', filename)
    
    # 限制文件名长度
    if len(filename) > Config.MAX_TITLE_LENGTH:
        filename = filename[:Config.MAX_TITLE_LENGTH]
    
    # 确保文件名不为空
    if not filename or filename == '.':
        return "未命名"
    
    return filename

def validate_title(title):
    """使用关键词验证标题的有效性"""
    if not title or len(title.strip()) < 5:
        return False
    
    # 转换为小写进行关键词匹配
    title_lower = title.lower()
    
    # 检查是否包含常见的标题关键词
    for keyword in Config.TITLE_KEYWORDS:
        if keyword.lower() in title_lower:
            return True
    
    # 如果标题长度适中且包含多个单词，也可能是有效标题
    words = title.split()
    if len(words) >= 3 and len(title) > 15:
        return True
    
    return False

def extract_institution(text):
    """从文本中提取机构信息"""
    if not text:
        return None
    
    text_lower = text.lower()
    
    # 查找包含机构关键词的行
    lines = text.split('\n')
    for line in lines:
        line_lower = line.lower().strip()
        for keyword in Config.INSTITUTION_KEYWORDS:
            if keyword.lower() in line_lower:
                # 提取可能的机构名称（通常是包含关键词的整个行）
                return line.strip()[:100]  # 限制长度
    
    return None

def extract_title_from_metadata(pdf_path):
    """从PDF元数据中提取标题"""
    try:
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            
            # 检查PDF是否加密
            if reader.is_encrypted:
                try:
                    # 尝试空密码解密
                    reader.decrypt('')
                    logger.info(f"PDF已加密，但成功使用空密码解密: {pdf_path}")
                except:
                    logger.warning(f"无法解密加密的PDF文件: {pdf_path}")
                    return None, "encrypted"
            
            if reader.metadata:
                # 尝试从不同的元数据字段获取标题
                title_fields = ['/Title', '/Subject', '/Topic', '/Keywords']
                for field in title_fields:
                    title = reader.metadata.get(field, '')
                    if title and title.strip() and title.strip().lower() not in ['', 'untitled', 'unnamed', 'no title']:
                        cleaned_title = title.strip()
                        # 使用关键词验证标题
                        if validate_title(cleaned_title) or field == '/Title':
                            return cleaned_title, None
    except PyPDF2.errors.PdfReadError as e:
        logger.error(f"PDF文件可能已损坏: {pdf_path} - {str(e)}")
        return None, "corrupted"
    except Exception as e:
        logger.warning(f"从元数据提取标题失败 {pdf_path}: {str(e)}")
        return None, str(e)
    return None, None

def extract_title_from_content(pdf_path):
    """从PDF内容中提取标题，使用多区域识别算法"""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 存储不同区域提取的文本
            region_texts = {
                'full': '',
                'header': '',
                'content': '',
                'footer': ''
            }
            
            # 读取前几页的文本，按区域提取
            num_pages = min(Config.MAX_PAGES_TO_CHECK, len(pdf.pages))
            for i in range(num_pages):
                page = pdf.pages[i]
                page_height = page.height
                page_width = page.width
                
                # 提取完整页面文本
                full_text = page.extract_text() or ''
                region_texts['full'] += full_text
                
                # 提取页眉区域文本
                header_box = (
                    page_width * Config.HEADER_REGION[0],
                    page_height * Config.HEADER_REGION[1],
                    page_width * Config.HEADER_REGION[2],
                    page_height * Config.HEADER_REGION[3]
                )
                header_page = page.within_bbox(header_box)
                region_texts['header'] += header_page.extract_text() or ''
                
                # 提取正文区域文本（最可能包含标题）
                content_box = (
                    page_width * Config.CONTENT_REGION[0],
                    page_height * Config.CONTENT_REGION[1],
                    page_width * Config.CONTENT_REGION[2],
                    page_height * Config.CONTENT_REGION[3]
                )
                content_page = page.within_bbox(content_box)
                region_texts['content'] += content_page.extract_text() or ''
                
                # 提取页脚区域文本
                footer_box = (
                    page_width * Config.FOOTER_REGION[0],
                    page_height * Config.FOOTER_REGION[1],
                    page_width * Config.FOOTER_REGION[2],
                    page_height * Config.FOOTER_REGION[3]
                )
                footer_page = page.within_bbox(footer_box)
                region_texts['footer'] += footer_page.extract_text() or ''
            
            # 首先尝试从正文区域提取标题（最可能包含真实标题）
            title = extract_title_from_text(region_texts['content'], 'content')
            if title and validate_title(title):
                return title, None, extract_institution(region_texts['full'])
            
            # 如果正文区域没有找到有效标题，尝试从完整页面提取
            title = extract_title_from_text(region_texts['full'], 'full')
            if title and validate_title(title):
                return title, None, extract_institution(region_texts['full'])
            
            # 如果仍然没有找到，尝试从页眉区域（学术论文标题有时在页眉）
            title = extract_title_from_text(region_texts['header'], 'header')
            if title and validate_title(title):
                return title, None, extract_institution(region_texts['full'])
            
            # 最后尝试从页脚区域
            title = extract_title_from_text(region_texts['footer'], 'footer')
            if title and validate_title(title):
                return title, None, extract_institution(region_texts['full'])
                
            # 如果所有区域都没找到，尝试返回任何有意义的文本
            all_lines = []
            for region_text in region_texts.values():
                if region_text:
                    lines = region_text.split('\n')
                    meaningful_lines = [line.strip() for line in lines 
                                      if line.strip() and len(line.strip()) > 5]
                    all_lines.extend(meaningful_lines)
            
            if all_lines:
                # 选择最长的非数字行作为最后的尝试
                candidate_lines = [line for line in all_lines[:10] if not line.isdigit()]
                if candidate_lines:
                    return max(candidate_lines, key=len), "low_confidence", extract_institution(region_texts['full'])
    
    except pdfplumber.openers.PDFSyntaxError as e:
        logger.error(f"PDF语法错误，文件可能已损坏: {pdf_path} - {str(e)}")
        return None, "corrupted", None
    except Exception as e:
        logger.warning(f"从内容提取标题失败 {pdf_path}: {str(e)}")
        return None, str(e), None
    
    return None, "no_title_found", None

def extract_title_from_text(text, region_name=""):
    """从文本中提取标题的辅助函数"""
    if not text:
        return None
    
    # 分割文本为行
    lines = text.split('\n')
    
    # 过滤空行和很短的行
    meaningful_lines = [line.strip() for line in lines 
                      if line.strip() and len(line.strip()) > 3]
    
    if not meaningful_lines:
        return None
    
    # 策略1：查找可能的标题（通常在文档开头，较长，包含多个单词）
    candidate_titles = []
    for i, line in enumerate(meaningful_lines[:10]):  # 检查前10行
        # 跳过明显不是标题的行
        if re.search(r'\d{4}|vol\.|no\.|pp\.|et al|DOI|http|www|@|email|abstract|introduction', 
                    line, re.IGNORECASE):
            continue
        
        # 检查是否可能是标题
        words = line.split()
        if len(words) >= 3 and len(line) > 10:
            # 计算标题得分
            score = 0
            # 长度得分
            if 15 < len(line) < 200:
                score += 2
            # 单词数量得分
            if 4 < len(words) < 30:
                score += 2
            # 大写字母比例得分（标题通常大写比例较高）
            uppercase_ratio = sum(1 for c in line if c.isupper()) / max(len(line), 1)
            if 0.2 < uppercase_ratio < 0.8:
                score += 1
            # 关键词匹配得分
            for keyword in Config.TITLE_KEYWORDS:
                if keyword.lower() in line.lower():
                    score += 3
                    break
            
            candidate_titles.append((line, score))
    
    # 如果找到候选标题，返回得分最高的
    if candidate_titles:
        # 按得分排序
        candidate_titles.sort(key=lambda x: x[1], reverse=True)
        return candidate_titles[0][0]
    
    # 策略2：如果没有找到候选标题，尝试返回第一行较长的文本
    for line in meaningful_lines[:5]:
        if len(line) > 15 and not line.isdigit():
            return line
    
    # 策略3：返回第一行有意义的文本
    return meaningful_lines[0] if meaningful_lines else None

def extract_title(pdf_path):
    """综合多种方法提取PDF标题，返回标题、状态和机构信息"""
    # 首先尝试从元数据提取
    metadata_title, metadata_status = extract_title_from_metadata(pdf_path)
    
    # 处理特殊状态
    if metadata_status == "encrypted":
        logger.warning(f"PDF文件已加密，无法提取标题: {pdf_path}")
        return None, "encrypted", None
    elif metadata_status == "corrupted":
        logger.error(f"PDF文件可能已损坏，无法提取标题: {pdf_path}")
        return None, "corrupted", None
    
    # 如果元数据提取成功
    if metadata_title:
        logger.info(f"从元数据成功提取标题: {metadata_title}")
        # 再次尝试提取机构信息（元数据函数不返回机构信息）
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if pdf.pages:
                    page_text = pdf.pages[0].extract_text() or ''
                    institution = extract_institution(page_text)
                    return metadata_title, "metadata", institution
        except:
            pass
        return metadata_title, "metadata", None
    
    # 如果元数据提取失败，尝试从内容提取
    content_title, content_status, institution = extract_title_from_content(pdf_path)
    
    if content_title:
        if content_status == "low_confidence":
            logger.info(f"从内容提取到低可信度标题: {content_title}")
        else:
            logger.info(f"从内容成功提取标题: {content_title}")
        return content_title, "content" if not content_status else content_status, institution
    
    logger.warning(f"无法提取标题: {pdf_path}")
    return None, "extraction_failed", None

def extract_year(text):
    """从文本中提取年份信息"""
    if not text:
        return None
    
    # 遍历所有年份模式
    for pattern in Config.YEAR_PATTERNS:
        matches = re.findall(pattern, text)
        if matches:
            # 提取完整的年份数字
            full_matches = re.findall(r'\b(19|20)\d{2}\b', text)
            if full_matches:
                # 选择最近的年份（如果有多个）
                years = sorted([int(match) for match in full_matches], reverse=True)
                return str(years[0])
    
    # 如果没有找到，尝试从当前日期获取年份作为默认值
    return datetime.now().strftime("%Y")

def extract_author(text):
    """从文本中提取作者信息"""
    if not text:
        return "Unknown"
    
    # 遍历所有作者模式
    for pattern in Config.AUTHOR_PATTERNS:
        matches = re.search(pattern, text, re.IGNORECASE)
        if matches:
            author = matches.group(1).strip()
            # 如果是多个作者，只取第一个
            if ',' in author:
                author = author.split(',')[0].strip()
            # 限制作者名长度
            return author[:20]
    
    # 如果找不到作者，尝试从文件名中提取
    # 通常论文文件名开头可能是作者姓
    return "Unknown"

def extract_keywords_from_title(title):
    """从标题中提取关键词（简洁版本的标题）"""
    if not title:
        return "Untitled"
    
    # 转换为小写并分割为单词
    words = title.lower().split()
    
    # 过滤常见的停用词和太短的词
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'is', 'are', 'was', 'were', 'be'}
    keywords = [word for word in words if word not in stop_words and len(word) >= Config.KEYWORD_MIN_LENGTH]
    
    # 限制关键词数量和长度
    result = ""
    current_length = 0
    for word in keywords[:5]:  # 最多取5个关键词
        if current_length + len(word) <= Config.MAX_KEYWORDS_LENGTH:
            if result:
                result += '_'
            result += word
            current_length += len(word) + 1  # +1 for the underscore
        else:
            break
    
    # 确保至少有一个关键词
    if not result:
        # 如果没有合适的关键词，返回标题的前几个单词
        result = '_'.join(words[:3])[:Config.MAX_KEYWORDS_LENGTH]
    
    return result

def generate_paper_filename(original_filename, title=None, text_content=None, year=None, author=None):
    """生成符合规范的论文文件名 - 仅包含标题部分"""
    # 如果标题为空，返回默认名称
    if not title or title.strip() == "":
        title = "未知标题"
    
    # 清理标题中的特殊字符和无效字符
    cleaned_title = sanitize_filename(title.strip())
    
    # 应用命名格式（仅包含标题）
    filename = Config.PAPER_NAMING_FORMAT.format(
        title=cleaned_title
    )
    
    # 限制文件名长度
    max_length = Config.MAX_TITLE_LENGTH - 4  # 减去.pdf扩展名的长度
    if len(filename) > max_length:
        # 直接截断标题
        cleaned_title = cleaned_title[:max_length]
        filename = Config.PAPER_NAMING_FORMAT.format(
            title=cleaned_title
        )
    
    # 移除文件名开头或结尾的下划线
    filename = filename.strip('_')
    
    return filename + '.pdf'

def generate_default_filename(timestamp=None):
    """生成默认文件名"""
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return Config.DEFAULT_NAME_FORMAT.format(timestamp=timestamp) + '.pdf'

def rename_pdfs(root_folder, recursive=True, custom_unknown_name=None, generate_excel=True):
    """重命名文件夹中的所有PDF文件
    
    Args:
        root_folder: 要处理的根文件夹路径
        recursive: 是否递归处理子文件夹
        custom_unknown_name: 用户自定义的未知文件命名前缀
        generate_excel: 是否生成Excel汇总表
    """
    # 检查文件夹是否存在
    if not os.path.exists(root_folder):
        logger.error(f"文件夹不存在: {root_folder}")
        return None
    
    # 初始化统计信息
    stats = {
        'total_files': 0,
        'pdf_files': 0,
        'renamed_files': 0,
        'failed_files': 0,
        'skipped_files': 0,
        'encrypted_files': 0,
        'corrupted_files': 0
    }
    
    # 用于跟踪全局重复标题
    global_title_counts = defaultdict(int)
    
    # 用于存储Excel数据
    excel_data = []
    
    # 递归遍历函数
    def process_directory(directory):
        # 跟踪当前目录中的文件信息
        dir_files = []
        
        # 遍历当前目录
        try:
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)
                
                # 如果是子文件夹且需要递归处理
                if os.path.isdir(item_path) and recursive:
                    process_directory(item_path)
                    continue
                
                # 跳过非文件项
                if not os.path.isfile(item_path):
                    continue
                
                stats['total_files'] += 1
                
                # 检查是否需要跳过（基于模式）
                skip = False
                for pattern in Config.SKIP_PATTERNS:
                    if re.match(pattern, item):
                        logger.info(f"跳过匹配模式的文件: {item_path}")
                        stats['skipped_files'] += 1
                        skip = True
                        break
                
                if skip:
                    continue
                
                # 只处理PDF文件
                if not item.lower().endswith('.pdf'):
                    logger.info(f"跳过非PDF文件: {item_path}")
                    stats['skipped_files'] += 1
                    continue
                
                stats['pdf_files'] += 1
                logger.info(f"处理文件: {item_path}")
                
                # 记录处理开始时间
                processing_time = datetime.now()
                timestamp = processing_time.strftime("%Y-%m-%d %H:%M:%S")
                
                # 初始化文件信息
                file_info = {
                    'original_filename': item,
                    'original_path': directory,
                    'extracted_title': None,
                    'institution': None,
                    'new_filename': item,
                    'author': 'Unknown',
                    'year': datetime.now().strftime("%Y"),
                    'keywords': 'Untitled',
                    'status': 'skipped',
                    'timestamp': timestamp,
                    'error_message': None
                }
                
                try:
                    # 提取标题、状态和机构信息
                    title, status, institution = extract_title(item_path)
                    file_info['extracted_title'] = title
                    file_info['institution'] = institution
                    
                    # 确定新文件名
                    if not title:
                        # 处理无法提取标题的情况
                        if status == "encrypted":
                            stats['encrypted_files'] += 1
                            file_info['status'] = 'encrypted'
                            logger.warning(f"文件已加密，跳过处理: {item_path}")
                        elif status == "corrupted":
                            stats['corrupted_files'] += 1
                            file_info['status'] = 'corrupted'
                            logger.error(f"文件已损坏，跳过处理: {item_path}")
                        else:
                            # 使用默认命名规则或用户自定义名称
                            if custom_unknown_name:
                                base_name = sanitize_filename(f"{custom_unknown_name}_{timestamp}")
                            else:
                                base_name = generate_default_filename(processing_time.strftime("%Y%m%d_%H%M%S"))
                            
                            new_filename = base_name + '.pdf'
                            file_info['new_filename'] = new_filename
                            file_info['status'] = 'default_named'
                            stats['failed_files'] += 1
                            logger.warning(f"无法提取标题，使用默认命名: {new_filename}")
                    else:
                        # 成功提取标题，处理重命名
                        # 尝试从文件内容提取更多信息
                        text_content = None
                        try:
                            with pdfplumber.open(item_path) as pdf:
                                # 读取前几页的文本用于提取年份和作者
                                text_content = ""
                                for page in pdf.pages[:3]:  # 只读取前3页
                                    if page.extract_text():
                                        text_content += page.extract_text()
                        except:
                            pass  # 如果读取失败，继续使用已有信息
                        
                        # 使用新的论文命名函数生成文件名 - 仅包含标题
                        new_filename = generate_paper_filename(
                            original_filename=item,
                            title=title,
                            text_content=text_content
                        )
                        
                        # 从生成的文件名中提取标题信息
                        # 文件名格式：标题.pdf
                        file_info['title'] = new_filename[:-4]  # 保存标题信息（去掉.pdf扩展名）
                        
                        # 全局标题计数，用于处理重复标题
                        global_title_counts[new_filename] += 1
                        if global_title_counts[new_filename] > 1:
                            # 使用"文件名_序号"格式处理重复
                            base_name, ext = os.path.splitext(new_filename)
                            new_filename = f"{base_name}_{global_title_counts[new_filename] - 1}{ext}"
                        
                        file_info['new_filename'] = new_filename
                        file_info['status'] = 'success' if status in ['metadata', 'content'] else status
                    
                    # 如果需要重命名
                    if file_info['new_filename'] != item:
                        new_file_path = os.path.join(directory, file_info['new_filename'])
                        
                        # 确保不会覆盖现有文件（额外的安全检查）
                        counter = 1
                        original_new_filename = file_info['new_filename']
                        while os.path.exists(new_file_path):
                            name, ext = os.path.splitext(original_new_filename)
                            # 避免与之前的序号冲突，使用下划线作为分隔符
                            if '-' in name and name.split('-')[-1].isdigit():
                                name_parts = name.split('-')[:-1]
                                name = '-'.join(name_parts)
                            new_filename = f"{name}_{counter}{ext}"
                            new_file_path = os.path.join(directory, new_filename)
                            counter += 1
                        
                        # 更新最终的新文件名
                        file_info['new_filename'] = new_filename
                        
                        # 执行重命名
                        try:
                            shutil.move(item_path, new_file_path)
                            logger.info(f"成功重命名: {item} -> {new_filename}")
                            stats['renamed_files'] += 1
                        except Exception as e:
                            error_msg = f"重命名失败: {str(e)}"
                            logger.error(f"{error_msg} - {item_path}")
                            file_info['status'] = 'rename_failed'
                            file_info['error_message'] = error_msg
                            stats['failed_files'] += 1
                except Exception as e:
                    # 捕获所有其他异常
                    error_msg = f"处理过程异常: {str(e)}"
                    logger.error(f"{error_msg} - {item_path}")
                    file_info['status'] = 'processing_error'
                    file_info['error_message'] = error_msg
                    stats['failed_files'] += 1
                
                # 添加到Excel数据
                excel_data.append(file_info)
        
        except Exception as e:
            logger.error(f"处理目录时发生错误 {directory}: {str(e)}")
            logger.debug(traceback.format_exc())
    
    # 开始处理
    logger.info(f"开始处理文件夹: {root_folder}")
    if recursive:
        logger.info("将递归处理所有子文件夹")
    process_directory(root_folder)
    
    # 生成Excel报告
    excel_file_path = None
    if generate_excel and excel_data:
        try:
            # 创建DataFrame
            df = pd.DataFrame(excel_data)
            
            # 调整列顺序 - 移除年份相关列，因为文件名现在只包含标题
            column_order = ['original_filename', 'new_filename', 'original_path', 
                           'extracted_title', 'title', 
                           'institution', 'status', 'timestamp', 'error_message']
            
            # 确保所有列都存在
            for col in column_order:
                if col not in df.columns:
                    df[col] = ''
            
            df = df[column_order]
            
            # 生成Excel文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            excel_file_name = f"论文信息汇总_{timestamp}.xlsx"
            excel_file_path = os.path.join(log_dir, excel_file_name)
            
            # 保存Excel文件
            with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='论文信息汇总')
                
                # 格式化工作表
                worksheet = writer.sheets['论文信息汇总']
                
                # 设置列宽 - 移除年份列的设置，因为文件名现在只包含标题
                column_widths = {
                    'original_filename': 30,
                    'new_filename': 30,
                    'original_path': 40,
                    'extracted_title': 50,
                    'title': 45,
                    'institution': 25,
                    'status': 15,
                    'timestamp': 20,
                    'error_message': 60
                }
                
                # 重命名列标题为中文
                for cell in worksheet[1]:  # 标题行
                    column_name = cell.value
                    if column_name == 'original_filename':
                        cell.value = '原文件名'
                    elif column_name == 'new_filename':
                        cell.value = '新文件名'
                    elif column_name == 'original_path':
                        cell.value = '文件夹路径'
                    elif column_name == 'extracted_title':
                        cell.value = '提取的标题'
                    elif column_name == 'title':
                        cell.value = '文件名标题'
                    elif column_name == 'institution':
                        cell.value = '机构信息'
                    elif column_name == 'status':
                        cell.value = '状态'
                    elif column_name == 'timestamp':
                        cell.value = '处理时间'
                    elif column_name == 'error_message':
                        cell.value = '错误信息'
                    
                    # 设置标题行字体加粗（使用更通用的方法）
                    try:
                        # 尝试导入xlsxwriter并使用正确的方法设置字体
                        import xlsxwriter
                        cell.font = xlsxwriter.format.Format({'bold': True})
                    except:
                        # 如果失败，继续而不设置字体（保持兼容性）
                        pass
                
                # 设置每列的宽度
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
            
            logger.info(f"Excel报告已生成: {excel_file_path}")
        except Exception as e:
            logger.error(f"生成Excel报告失败: {str(e)}")
            logger.debug(traceback.format_exc())
    
    # 输出详细统计信息
    logger.info(f"\n===== 处理统计 =====")
    logger.info(f"总文件数: {stats['total_files']}")
    logger.info(f"PDF文件数: {stats['pdf_files']}")
    logger.info(f"成功重命名: {stats['renamed_files']}")
    logger.info(f"标题提取失败: {stats['failed_files']}")
    logger.info(f"跳过文件: {stats['skipped_files']}")
    logger.info(f"加密文件: {stats['encrypted_files']}")
    logger.info(f"损坏文件: {stats['corrupted_files']}")
    logger.info(f"日志文件: {log_file}")
    if excel_file_path:
        logger.info(f"Excel报告: {excel_file_path}")
    logger.info(f"====================\n")
    
    # 控制台输出摘要
    print(f"\n===== 处理完成 =====")
    print(f"总文件数: {stats['total_files']}")
    print(f"PDF文件数: {stats['pdf_files']}")
    print(f"成功重命名: {stats['renamed_files']}")
    print(f"标题提取失败: {stats['failed_files']}")
    print(f"跳过文件: {stats['skipped_files']}")
    print(f"加密文件: {stats['encrypted_files']}")
    print(f"损坏文件: {stats['corrupted_files']}")
    print(f"日志文件: {log_file}")
    if excel_file_path:
        print(f"Excel报告: {excel_file_path}")
    print(f"====================\n")
    
    return excel_file_path

def main():
    """主函数，处理命令行参数并执行程序"""
    # 检查必要的依赖库
    required_libs = {
        'PyPDF2': 'PyPDF2',
        'pdfplumber': 'pdfplumber',
        'pandas': 'pandas',
        'openpyxl': 'openpyxl'
    }
    
    missing_libs = []
    for lib_name, install_name in required_libs.items():
        try:
            __import__(lib_name)
        except ImportError:
            missing_libs.append(install_name)
    
    if missing_libs:
        print(f"错误: 缺少必要的依赖库。")
        print(f"缺失的库: {', '.join(missing_libs)}")
        print(f"安装命令: pip install {' '.join(missing_libs)}")
        return 1
    
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(
        description='论文文件规范化命名与信息提取工具',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    # 添加必要的参数
    parser.add_argument('folder_path', nargs='?', 
                        help='要处理的论文文件夹路径')
    
    # 添加可选参数
    parser.add_argument('--recursive', '-r', action='store_true', 
                        help='递归处理所有子文件夹')
    
    parser.add_argument('--custom-name', '-n', 
                        help='无法识别标题时使用的自定义命名前缀')
    
    parser.add_argument('--no-excel', action='store_true', 
                        help='不生成Excel汇总报告')
    
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default='INFO', help='日志级别（默认: INFO）')
    
    parser.add_argument('--config', help='自定义配置文件路径（暂未实现）')
    
    # 解析参数
    args = parser.parse_args()
    
    # 如果未提供文件夹路径，则通过输入获取
    folder_path = args.folder_path
    if not folder_path:
        # 打印程序信息
        print("===== 论文文件规范化命名与信息提取工具 =====")
        print("本程序将扫描指定文件夹中的PDF论文文件，提取论文信息（标题、作者、年份等）")
        print("并按照'作者_年份_关键词'的格式进行规范化重命名，同时生成论文信息Excel汇总表。")
        print("===========================================\n")
        print("使用方法:")
        print("  python pdf_title_renamer.py [文件夹路径] [选项]")
        print("\n选项:")
        print("  -r, --recursive    递归处理所有子文件夹")
        print("  -n, --custom-name  无法识别标题时使用的自定义命名前缀")
        print("  --no-excel         不生成Excel汇总报告")
        print("  --log-level        设置日志级别（DEBUG, INFO, WARNING, ERROR）")
        print("\n示例:")
        print("  python pdf_title_renamer.py D:/pdfs -r")
        print("  python pdf_title_renamer.py D:/pdfs --custom-name 未命名论文")
        print("\n")
        
        folder_path = input("请输入要处理的文件夹路径: ")
    
    # 更新日志级别
    if hasattr(args, 'log_level'):
        logger.setLevel(getattr(logging, args.log_level))
    
    # 调用重命名函数
    try:
        # 转换路径格式（适用于Windows系统）
        folder_path = os.path.abspath(folder_path)
        rename_pdfs(
            root_folder=folder_path,
            recursive=args.recursive,
            custom_unknown_name=args.custom_name,
            generate_excel=not args.no_excel
        )
        return 0
    except KeyboardInterrupt:
        print("\n程序已被用户中断。")
        logger.info("程序已被用户中断")
        return 130
    except Exception as e:
        print(f"\n程序执行出错: {str(e)}")
        logger.error(f"程序执行出错: {str(e)}")
        logger.debug(traceback.format_exc())
        return 1


if __name__ == "__main__":
    # 确保日志目录存在
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    # 配置日志
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"pdf_renamer_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger()
    
    # 运行主函数
    exit_code = main()
    exit(exit_code)