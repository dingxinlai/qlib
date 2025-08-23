import pandas as pd
import numpy as np
import os
import glob
from tqdm import tqdm

def process_stock_data(stock_code, no_adj_dir, hfq_dir, qfq_dir, output_dir):
    """
    处理单只股票的数据转换
    
    Args:
        stock_code: 股票代码 (如 '000001')
        no_adj_dir: 不复权数据目录
        hfq_dir: 后复权数据目录
        qfq_dir: 前复权数据目录
        output_dir: 输出目录
    """
    try:
        # 确定交易所前缀
        if stock_code.startswith('6'):
            exchange = 'SH'
        else:
            exchange = 'SZ'
            
        # 构建文件路径
        no_adj_file = os.path.join(no_adj_dir, f"{stock_code}_daily.csv")
        hfq_file = os.path.join(hfq_dir, f"{stock_code}_daily_hfq.csv")
        qfq_file = os.path.join(qfq_dir, f"{stock_code}_daily_qfq.csv")
        
        # 读取数据
        no_adj_df = pd.read_csv(no_adj_file)
        hfq_df = pd.read_csv(hfq_file)
        qfq_df = pd.read_csv(qfq_file)
        
        # 确保日期格式一致 - Qlib要求YYYY-MM-DD格式
        for df in [no_adj_df, hfq_df, qfq_df]:
            df['日期'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
        
        # 合并数据
        merged_df = no_adj_df[['日期', '股票代码', '成交量', '成交额']].copy()
        
        # 添加后复权价格数据
        merged_df = merged_df.merge(
            hfq_df[['日期', '开盘', '收盘', '最高', '最低']], 
            on='日期', 
            how='left',
            suffixes=('', '_hfq')
        )
        
        # 添加前复权价格数据
        merged_df = merged_df.merge(
            qfq_df[['日期', '开盘', '收盘', '最高', '最低']], 
            on='日期', 
            how='left',
            suffixes=('_hfq', '_qfq')
        )
        
        # 重命名列
        merged_df.rename(columns={
            '开盘_hfq': 'open',
            '收盘_hfq': 'close',
            '最高_hfq': 'high',
            '最低_hfq': 'low',
            '开盘_qfq': 'open_qfq',
            '收盘_qfq': 'close_qfq',
            '最高_qfq': 'high_qfq',
            '最低_qfq': 'low_qfq'
        }, inplace=True)
        
        # 添加股票代码
        merged_df['instrument'] = exchange + stock_code
        
        # 计算VWAP (成交量加权平均价)
        # 注意: 成交量单位是手，需要转换为股
        merged_df['volume'] = merged_df['成交量'] * 100  # 转换为股
        merged_df['amount'] = merged_df['成交额']  # 单位已经是元
        merged_df['vwap'] = merged_df['amount'] / merged_df['volume']
        
        # 处理除零错误
        merged_df['vwap'] = merged_df.apply(
            lambda x: x['vwap'] if x['volume'] > 0 else np.nan, 
            axis=1
        )
        
        # 计算复权因子 (使用后复权价格和原始价格)
        # 直接从原始数据获取不复权收盘价
        merged_df['close_no_adj'] = no_adj_df['收盘']
        
        # 计算复权因子: factor = 后复权价格 / 原始价格
        merged_df['factor'] = merged_df['close'] / merged_df['close_no_adj']
        
        # 选择Qlib所需的列（不包含instrument，文件名已包含股票代码）
        qlib_df = merged_df[[
            '日期', 'open', 'high', 'low', 'close', 
            'volume', 'amount', 'vwap', 'factor'
        ]].copy()
        
        qlib_df.rename(columns={'日期': 'date'}, inplace=True)
        
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存为CSV文件
        output_file = os.path.join(output_dir, f"{exchange}{stock_code}.csv")
        qlib_df.to_csv(output_file, index=False)
        
        return True
        
    except Exception as e:
        print(f"处理股票 {stock_code} 时出错: {str(e)}")
        return False

def main():
    # 设置路径
    no_adj_dir = r"D:\BaiduNetdiskDownload\daily"  # 不复权数据目录
    hfq_dir = r"D:\BaiduNetdiskDownload\daily_hfq"  # 后复权数据目录
    qfq_dir = r"D:\BaiduNetdiskDownload\daily_qfq"  # 前复权数据目录
    output_dir = r"D:\data"  # 输出目录
    
    # 获取所有股票代码
    no_adj_files = glob.glob(os.path.join(no_adj_dir, "*_daily.csv"))
    stock_codes = [os.path.basename(f).split("_")[0] for f in no_adj_files]
    
    print(f"找到 {len(stock_codes)} 只股票的数据")
    
    # 处理所有股票
    success_count = 0
    for stock_code in tqdm(stock_codes, desc="处理股票数据"):
        if process_stock_data(stock_code, no_adj_dir, hfq_dir, qfq_dir, output_dir):
            success_count += 1
    
    print(f"处理完成! 成功处理 {success_count}/{len(stock_codes)} 只股票的数据")
    
    # 生成Qlib的instruments.txt文件
    generate_instruments_file(output_dir)

def generate_instruments_file(data_dir):
    """生成Qlib所需的instruments.txt文件"""
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    instruments = [os.path.basename(f).split(".")[0] for f in csv_files]
    
    instruments_file = os.path.join(data_dir, "instruments.txt")
    with open(instruments_file, 'w') as f:
        for instrument in instruments:
            f.write(f"{instrument}\n")
    
    print(f"已生成 {instruments_file}，包含 {len(instruments)} 只股票")

if __name__ == "__main__":
    main()