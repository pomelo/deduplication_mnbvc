from argparse import ArgumentParser
from asyncio import run, Queue, create_task, get_running_loop, wait_for
from concurrent.futures import ProcessPoolExecutor
from itertools import chain, islice
from os import getpid
from pathlib import Path
from time import perf_counter
from typing import Optional, Any

import hashlib
from jsonlines import open
import picologging as logging
import simhash
from tqdm import tqdm


# setup logger with PID for process pool
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()


def from_txt_to_json(file: Path, arguments: Any):
    # 定义json结构
    file_json = {'文件名': file.as_posix(),
                 '是否待查文件': False,
                 '是否重复文件': False,
                 '文件大小': file.stat().st_size,
                 'simhash': 0,
                 '最长段落长度': 0,
                 '段落数': 0,
                 '去重段落数': 0,
                 '低质量段落数': 0,
                 '段落': []}
    # 定义用于去重的set
    hash_set = set()
    texts = set()

    # 读取每一行
    with file.open('r', encoding='utf-8', errors='strict') as f:
        for line_number, line in enumerate(f):
            # 去除行首尾空格, 跳过空行
            if not (line := line.strip()):
                continue
            # 计算最长段落长度
            file_json['最长段落长度'] = max(file_json['最长段落长度'], len(line))
            # 将每一行内容添加到json中
            file_json['段落'].append({
                '行号': line_number,
                'md5': (md5 := hashlib.md5(line.encode()).hexdigest()),
                '是否重复': md5 in hash_set,
                '是否跨文件重复': False,
                '内容': line
            })
            texts.add(line)
            # 将md5值添加到set中，用于去重
            hash_set.add(md5)

    file_json['去重段落数'] = len(hash_set)
    if not file_json['去重段落数']:
        return None
    # 计算段落数和去重段落数
    file_json['段落数'] = len(file_json['段落'])
    # 计算simhash
    file_json['simhash'] = simhash.Simhash(texts).value
    # 判断是否是待查文件
    if (file_json['去重段落数'] / file_json['段落数']) < arguments.threshold:
        file_json['是否待查文件'] = True
    return file_json


def hash_processor(file: Path, arguments: Any):
    """将源文件按行hash, 并计算simhash"""

    # use relative path to identify the file
    output_file = (
        Path(arguments.dst_dir) /
        f'{str(file.relative_to(file.root)).strip(" ./").replace(".", "").replace("/", "_")}.{arguments.dst}'
    )
    output_review_file = (
        Path(arguments.dst_dir) /
        f'{"problem_"}'
        f'{str(file.relative_to(file.root)).strip(" ./").replace(".", "").replace("/", "_")}.{arguments.dst}'
    )

    # skip processed file
    if output_file.exists() or output_review_file.exists():
        logger.warning(f'{file.resolve()} has been processed')
    else:
        try:
            json = from_txt_to_json(file, arguments)

            if json:
                output_file_name = output_review_file.resolve() if json.get('是否待查文件') else output_file.resolve()
                with open(output_file_name, 'a') as f:
                    f.write(json)
        except UnicodeDecodeError:
            logging.error(f"Error: {str(file)} is not encoded in utf-8.")

    return getpid()


async def process_file(
    queue: Queue,
    executor: ProcessPoolExecutor,
    arguments: Any,
    global_bar: tqdm
):
    """Process file contents using Process Pool"""

    has_reset = False
    while True:
        try:
            # get a file
            file: path = await queue.get()
            # process it in worker process
            loop = get_running_loop()
            future = loop.run_in_executor(executor, hash_processor, file, arguments)
            sub_pid = await wait_for(future, 60)
            # update global progress bar
            global_bar.set_description_str(f'{getpid()} | 当前处理文件：{file.name}，总体处理进度')
            global_bar.update()
        except Exception as e:
            logger.error(f'{e}: {type(e)}')
            break

        queue.task_done()


async def get_file_list(
    arguments: Any,
    queue: Queue,
    global_bar: tqdm,
    legal_file_type: Optional[str] = ['.txt']
):
    """Recursively iterate through all subdirectories and put files into the task queue"""

    path = Path(arguments.src_dir)

    all_files = [x for x in path.rglob('*') if x.suffix in legal_file_type]
    file_count = len(all_files)

    # Setup progress bars
    global_bar.reset(file_count)
    setattr(arguments, 'total', file_count)

    # Add files to queue
    for file in all_files:
        await queue.put(file)


async def convert(arguments: Any):
    """Main function, start child processes to check content replication"""

    # 检查输入参数是否合理
    if not Path(arguments.src_dir).exists():
        logger.error('源文件目录不正确')
        return

    # 如果源文件和目标文件类型不匹配，则抛出异常
    if arguments.src != 'txt' or arguments.dst != 'jsonl':
        logger.error('Only support converting from txt to jsonl now.')
        return

    # 如果输出目录不存在，则创建
    Path(arguments.dst_dir).mkdir(parents=True, exist_ok=True)

    # 获取源文件列表
    file_queue = Queue()

    # Process pool executor
    executor = ProcessPoolExecutor(max_workers=arguments.n_process)
    # Progress bar for overall progress
    global_bar = tqdm()
    # create the consumer
    consumers = [
        create_task(process_file(file_queue, executor, arguments, global_bar))
        for _ in range(arguments.n_process)
    ]
    # create the producer and wait for it to finish
    await create_task(get_file_list(arguments, file_queue, global_bar))

    # wait for all files to be processed, await for the consumer implicitly
    await file_queue.join()
    # Stop the infinite loop in the consumer
    for consumer in consumers:
        consumer.cancel()
    # shutdown the process pool
    executor.shutdown()


if __name__ == '__main__':
    # 设置参数解析器
    parser = ArgumentParser()
    # 添加必须指定的参数
    parser.add_argument('--src_dir', type=str, required=True, help='源文件夹路径')
    # 添加可选参数，指定源文件类型，默认为txt
    parser.add_argument('--src', type=str, default='txt', help='指定源文件类型，默认为txt')
    # 添加可选参数，指定目标文件类型，默认为jsonl
    parser.add_argument('--dst', type=str, default='jsonl', help='指定目标文件类型，默认为jsonl')
    # 添加可选参数，指定转换后文件存放路径，默认为./converted
    parser.add_argument('--dst_dir', type=str, default='converted', help='指定转换后文件存放路径，默认为./converted')
    # 添加可选参数，指定进程数，默认为1
    parser.add_argument('--n_process', type=int, default=1, help='指定进程数，默认为1')
    # 添加可选参数，指定去重阈值，默认为0.95
    parser.add_argument('--threshold', type=float, default=0.95, help='指定去重阈值，默认为0.95')
    # 解析参数,调用convert函数
    args = parser.parse_args()

    tick = perf_counter()
    run(convert(args))
    logger.info(f'{args.src_dir} 下共 {args.total} 文件处理完毕. 用时 {perf_counter() - tick} 秒。')
