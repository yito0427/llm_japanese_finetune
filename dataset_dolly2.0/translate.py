import os
import argparse
import boto3
import json

# AWSリージョンを指定
region_name = 'us-west-1'

# 翻訳元の言語コード
source_language_code = 'en'

# 翻訳先の言語コード
target_language_code = 'ja'

# 与えられた文字列を10000文字以下に分割するPythonコード例です。
# 最大長が10000文字を超えている場合、最も近いピリオドの位置で分割します。
# このコードでは、与えられた文字列が10000文字以下であれば、そのままリストに追加します。
# 10000文字を超える場合は、最も近いピリオドの位置を見つけて、その位置で文字列を分割します。
# 分割された文字列は、リストに追加されます。最後に、リストを返します。
def split_string(string):
    allow_size = 9000
    if len(string) <= allow_size:
        return [string]

    result = []
    while len(string) > allow_size:
        idx = string.rfind('.', 0, allow_size)
        if idx == -1:
            idx = allow_size
        result.append(string[:idx])
        string = string[idx+1:]
    result.append(string)
    return result

# 9000以上の場合は分割して翻訳する
# 関数内でファイルを読み込む
# for Amazon Translate Quota, see below:
# https://docs.aws.amazon.com/translate/latest/dg/what-is-limits.html
def translate_en2ja(row_start, row_end, read_row_batch, input_jsonl, outdir_parts):
    translate = boto3.client('translate', region_name=region_name)
    data_ja = []
    count = 0
    
    # 指定された行番号のjsonlを処理していく
    with open(input_jsonl, 'r') as f:
        for i, line in enumerate(f):
            if i < row_start:
                continue
            if i > row_end:
                break
            print(i)
            data = json.loads(line)
            # instructionを翻訳する
            # 9000字を超えていたら、分割する
            # 翻訳するテキスト
            text = data['instruction']
    
            # 翻訳を実行:instruction
            text_list = split_string(data['instruction']) ### 9000字以上あったら分割
            instruction_ja=""
            for text_str in text_list:
                instruction_ja_tmp = translate.translate_text(
                    Text=text_str,
                    SourceLanguageCode=source_language_code,
                    TargetLanguageCode=target_language_code
                )['TranslatedText']
                instruction_ja+=instruction_ja_tmp
            
            # 翻訳を実行:context
            ### contextは、NULLのケースがある。例2行目
            if len(data['context']) == 0:
                context_ja = ""
            else:
                text_list = split_string(data['context']) ### 9000字以上あったら分割
                context_ja=""
                for text_str in text_list:
                    context_ja_tmp = translate.translate_text(
                        Text=text_str,
                        SourceLanguageCode=source_language_code,
                        TargetLanguageCode=target_language_code
                    )['TranslatedText']
                    context_ja+=context_ja_tmp
            
            # 翻訳を実行:response
            text_list = split_string(data['response']) ### 9000字以上あったら分割
            response_ja=""
            for text_str in text_list:
                response_ja_tmp = translate.translate_text(
                    Text=text_str,
                    SourceLanguageCode=source_language_code,
                    TargetLanguageCode=target_language_code
                )['TranslatedText']
                response_ja+=response_ja_tmp
            
            #print('instruction=====')
            #print(instruction_ja)
            #print('context=====')
            #print(context_ja)
            #print('response=====')
            #print(response_ja)
            
            data_ja += [{'row':i,
            'instruction':instruction_ja,
            'context':context_ja,
            'response':response_ja,
            'category':data['category']
            }]
            
            count += 1
           
            ### 指定のバッチ回数に到達したら、ファイルを書き出してリセット
            if count == read_row_batch and not i == row_end-1:
                print("====== Saving file for reaching batch num ======")
                ### save file
                with open(f'{outdir_parts}databricks-dolly-15k-ja_{i - read_row_batch + 1}_{i}.jsonl', 'w', encoding='utf-8') as f_out:
                    for d in data_ja:
                        json.dump(d, f_out, ensure_ascii=False)
                        f_out.write('\n')
                count = 0
                data_ja = []
           
            ### ENDに到達した場合も、ファイルを書き出して終了
            if i == row_end:
                print("====== Saving file for reaching the END ======")
                ### save file
                with open(f'{outdir_parts}databricks-dolly-15k-ja_{i - count + 1}_{i}.jsonl', 'w', encoding='utf-8') as f_out:
                    for d in data_ja:
                        json.dump(d, f_out, ensure_ascii=False)
                        f_out.write('\n')
                return
            
            
def concatenate_files(directory_path, output_file_path):
    """
    指定されたディレクトリ配下にあるファイルを結合して、指定されたファイルに出力する関数

    Parameters
    ----------
    directory_path : str
        入力ファイルが存在するディレクトリのパス
    output_file_path : str
        出力ファイルのパス
    """
    # 入力ファイルのパスを取得
    input_file_paths = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

    # 入力ファイルを結合して出力ファイルに書き込む
    with open(output_file_path, 'w') as output_file:
        for input_file_path in input_file_paths:
            with open(input_file_path, 'r') as input_file:
                output_file.write(input_file.read())

    print(f"{output_file_path}を作成しました。")


def get_info_jsonl(input_jsonl):
    length_inst=[]
    length_cntx=[]
    length_rspn=[]
    # 文字列の長さを格納していく。
    # read jsonl file
    with open(input_jsonl, 'r') as f:
        for line in f:
            #print(line)
            data = json.loads(line)
            length_inst.append(len(data['instruction']))
            length_cntx.append(len(data['context']))
            length_rspn.append(len(data['response']))

    print("memo: index begins from 1, not 0.")
    print(f"instruction : max_index={length_inst.index(max(length_inst))+1}") #1から始まるindexとした
    print(f"instruction : max_value={max(length_inst)}")
    print(f"instruction : min_index={length_inst.index(min(length_inst))+1}") #1から始まるindexとした
    print(f"instruction : min_value={min(length_inst)}")
    print(f"context : max_index={length_cntx.index(max(length_cntx))+1}") #1から始まるindexとした
    print(f"context : max_value={max(length_cntx)}")
    print(f"context : min_index={length_cntx.index(min(length_cntx))+1}") #1から始まるindexとした
    print(f"context : min_value={min(length_cntx)}")
    print(f"response : max_index={length_rspn.index(max(length_rspn))+1}") #1から始まるindexとした
    print(f"response : max_value={max(length_rspn)}")
    print(f"response : min_index={length_rspn.index(min(length_rspn))+1}") #1から始まるindexとした
    print(f"response : min_value={min(length_rspn)}")
    return len(length_inst) #ファイルの行数を返す


def main(args):
    # 引数を使った処理をここに記述する
    print(f"row_start: {args.row_start}")
    print(f"row_end: {args.row_end}")
    print(f"read_row_batch: {args.read_row_batch}")
    print(f"input_jsonl: {args.input_jsonl}")
    print(f"outdir_parts: {args.outdir_parts}")
    print(f"outdir_all: {args.outdir_all}")
    print("="*60)
    # 翻訳対象のjsonlの基礎情報を表示する。ファイルの行数も取得
    json_row_num = get_info_jsonl(args.input_jsonl)
    # 日本語に翻訳し、ファイルを格納
    print(f"number of row in jsonl:{json_row_num}")
    if args.row_start > json_row_num:
        print(f"Error: start is bigger than number of row in jsonl!! last index is {json_row_num - 1}")
    # ディレクトリ作成    
    if not os.path.exists(args.outdir_parts):
        os.mkdir(args.outdir_parts)
    print("===== Start translation =====")
    translate_en2ja(args.row_start, min(args.row_end, json_row_num), args.read_row_batch, args.input_jsonl, args.outdir_parts)
    print("===== Finish translation =====")
    # 翻訳ファイルを結合する。重複排除のみ実施
    # ディレクトリ作成    
    if not os.path.exists(args.outdir_all):
        os.mkdir(args.outdir_all)
    concatenate_files(args.outdir_parts, args.outdir_all+'databricks-dolly-15k-ja.jsonl')
    


if __name__ == '__main__':
    # 引数の解析器を作成する
    parser = argparse.ArgumentParser(description='Translate english jonl dataset into Japanese such as Dolly2.0 jsonl')
    # 引数の定義を追加する
    parser.add_argument('--row_start', help='description of arg1', default=1-1, type=int)
    parser.add_argument('--row_end', help='description of arg2', default=15015-1, type=int)
    parser.add_argument('--read_row_batch', help='description of arg2', default=100, type=int)
    parser.add_argument('--input_jsonl', help='description of arg2', default='./databricks-dolly-15k.jsonl')
    parser.add_argument('--outdir_parts', help='description of arg2', default='./output_parts/')
    parser.add_argument('--outdir_all', help='description of arg2', default='./output_all/')

    # 引数を解析する
    args = parser.parse_args()
    # 引数を main() 関数に渡す
    main(args)