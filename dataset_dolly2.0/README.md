EC2での実施では、エラーは発生しません。

$ python translate.py

Cloud9の場合、たまに以下のようなエラー（The security token included in the request is expired）が発生します。
出力完了したつぎから、--row_startを指定して再実行してください。
2100 - 2200 行くらい処理すると、エラーが発生します。

参考：Cloud9の認証方式は特殊です。
https://www.bioerrorlog.work/entry/cloud9-expired-token

=======================================================================
Traceback (most recent call last):
  File "translate3.py", line 229, in <module>
    main(args)
  File "translate3.py", line 205, in main
    translate_en2ja(args.row_start, min(args.row_end, json_row_num), args.read_row_batch, args.input_jsonl, args.outdir_parts)
  File "translate3.py", line 65, in translate_en2ja
    TargetLanguageCode=target_language_code
  File "/home/ec2-user/.local/lib/python3.7/site-packages/botocore/client.py", line 530, in _api_call
    return self._make_api_call(operation_name, kwargs)
  File "/home/ec2-user/.local/lib/python3.7/site-packages/botocore/client.py", line 960, in _make_api_call
    raise error_class(parsed_response, operation_name)
botocore.exceptions.ClientError: An error occurred (ExpiredTokenException) when calling the TranslateText operation: The security token included in the request is expired
demo:~/environment/dolly_translate2 $ python translate3.py --row_start=2100