import os
import pandas as pd
import glob
from collections import Counter
import threading
import json
import csv

# 猴子补丁，确保线程方法兼容
if not hasattr(threading.Thread, 'isAlive'):
    threading.Thread.isAlive = threading.Thread.is_alive


def process_files(input_dir='.', output_base='qa-1200'):
    # 获取所有响应文件
    response_files = glob.glob(os.path.join(input_dir, 'prompt_batch*_response.csv'))

    # 自然排序文件名
    response_files.sort(key=lambda x: int(''.join(filter(str.isdigit, os.path.basename(x)))))

    print(f"找到{len(response_files)}个response文件: {[os.path.basename(f) for f in response_files]}")
    print()

    # 创建一个空的DataFrame来保存所有数据
    all_data = pd.DataFrame(columns=['difficulty', 'question', 'answer'])

    # 记录总记录数
    total_records = 0

    # 用于记录错误数量
    error_count = 0

    # 遍历每个响应文件
    for file_path in response_files:
        print(f"处理文件: {file_path}")

        try:
            # 读取CSV文件，使用引擎'python'以处理复杂CSV
            df = pd.read_csv(file_path, engine='python')

            # 检查是否有空值
            has_null = df.isnull().any().any()
            if has_null:
                original_rows = len(df)
                df = df.dropna()
                new_rows = len(df)
                print(f"警告: {file_path} 有 {original_rows - new_rows} 行包含空值，将被移除")
                print(f"警告: 过滤后从 {original_rows} 行减少到 {new_rows} 行")

            # 检查difficulty列的值是否合法
            valid_difficulties = ['simple', 'complex', 'deep']
            invalid_difficulties = [d for d in df['difficulty'].unique() if d not in valid_difficulties]

            if invalid_difficulties:
                original_rows = len(df)
                print(f"错误: {file_path} 包含无效的difficulty值: {invalid_difficulties}")
                print(f"      有效值应为: {valid_difficulties}")
                df = df[df['difficulty'].isin(valid_difficulties)]
                new_rows = len(df)
                print(f"      共有 {original_rows - new_rows} 行数据受影响，将被移除")
                print(f"警告: 过滤后从 {original_rows} 行减少到 {new_rows} 行")
                error_count += 1

            # 添加到总数据中
            all_data = pd.concat([all_data, df], ignore_index=True)

            # 更新总记录数
            records_added = len(df)
            total_records += records_added
            print(f"成功添加 {records_added} 条记录，现有总记录 {total_records} 条")

            # 计算每种难度的问题数量
            difficulty_counts = Counter(df['difficulty'])
            print(f"此文件中各难度问题数量: {dict(difficulty_counts)}")

        except Exception as e:
            print(f"错误: 处理 {file_path} 时发生异常: {e}")
            error_count += 1

        print()

    # 统计最终数据中各难度的问题数量
    difficulty_counts = Counter(all_data['difficulty'])
    total = len(all_data)

    print(f"合并后总共有 {total} 条问答对")
    print("各难度问题数量统计:")
    for difficulty, count in difficulty_counts.items():
        percentage = count / total * 100
        print(f"- {difficulty}: {count}题 ({percentage:.1f}%)")

    # 按照难度排序：simple, complex, deep
    difficulty_order = {'simple': 0, 'complex': 1, 'deep': 2}
    all_data['sort_key'] = all_data['difficulty'].map(difficulty_order)
    all_data = all_data.sort_values('sort_key').drop('sort_key', axis=1)

    # 保存为多种格式

    # 1. 改进的CSV格式（引用所有字段，使用转义字符）
    csv_file = f"{output_base}.csv"
    all_data.to_csv(csv_file, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
    print(f"\n成功将所有数据保存为CSV: {csv_file}")

    # 2. TSV格式（Tab分隔值）
    # tsv_file = f"{output_base}.tsv"
    # all_data.to_csv(tsv_file, sep='\t', index=False)
    # print(f"成功将所有数据保存为TSV: {tsv_file}")

    # 3. JSON格式
    json_file = f"{output_base}.json"
    all_data_json = all_data.to_dict(orient='records')
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(all_data_json, f, ensure_ascii=False, indent=2)
    print(f"成功将所有数据保存为JSON: {json_file}")

    # 显示几条示例数据
    print("\n示例数据:")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 120)
    print(all_data.head(3))

    if error_count > 0:
        print(f"\n警告: 处理过程中共发现 {error_count} 个错误，请检查上述日志")


if __name__ == "__main__":
    process_files()