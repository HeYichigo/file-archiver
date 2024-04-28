import os
import operator
from datetime import datetime
from zipfile import ZipFile
from os import path
from concurrent.futures import ProcessPoolExecutor, Future
import logging
import uuid
import shutil

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(process)-6d %(processName)-16s %(levelname)s %(message)s",
)

root_path: str = os.sys.argv[1]
target_path: str = os.sys.argv[2]
limit: int = int(os.sys.argv[3])
executor = ProcessPoolExecutor()
d = datetime.now()
current_date = datetime(year=d.year, month=d.month, day=1)

future_set: list[Future] = []


def get_root_dir_list(root: str):
    """
    获得根目录中的文件夹列表
    """
    dir_list = [path.join(root, dir_name) for dir_name in os.listdir(root)]
    return dir_list


def get_file_list(dir_path: str):
    """
    获得文件夹中的文件列表
    """
    file_list = []
    for dirpath, _, filenames in os.walk(dir_path):
        if len(filenames) > 0:
            for file_name in filenames:
                file_path = path.join(dirpath, file_name)
                ctime = path.getmtime(file_path)
                file_time = datetime.fromtimestamp(ctime)
                year, month = file_time.year, file_time.month
                # 只取得每个文件月份的部分
                file_month_time = datetime(year, month, 1)
                item = (file_path, file_month_time)
                file_list.append(item)
    return sorted(file_list, key=operator.itemgetter(1))


def group_file_list(file_list: list[tuple[str, datetime]]):
    """
    从文件列表中分组进行压缩
    """
    if len(file_list) == 0:
        return
    s_idx = 0
    idx = 0
    s_date = file_list[0][1]
    ## 最后一个月份的不会被处理
    while idx < len(file_list):
        _, date = file_list[idx]
        if s_date != date or (idx - s_idx) > limit:
            s_date = date
            fut = executor.submit(handle_file_list, file_list, s_idx, idx)
            future_set.append(fut)
            s_idx = idx
        idx = idx + 1

    ## 检查最后一个月份，如果不是当月就处理
    if s_date != current_date:
        fut = executor.submit(handle_file_list, file_list, s_idx, idx)
        future_set.append(fut)


# 处理文件 [s_idx, e_idx)
def handle_file_list(file_list: list[tuple[str, datetime]], s_idx: int, e_idx: int):
    _, arch_name = file_list[s_idx]
    arch_name = f"{arch_name.year}-{arch_name.month:0>2}-archive"
    arch_path = path.join(target_path, arch_name)
    # 尝试创建文件夹
    try:
        os.mkdir(arch_path)
    except FileExistsError:
        pass
    # 将文件移动至该目录
    for idx in range(s_idx, e_idx):
        file_path, _ = file_list[idx]
        shutil.move(file_path, arch_path)
        i = (idx - s_idx) * 100 / (e_idx - s_idx)
        logger.info(f"{arch_name[0:20]}: {i:2.1f}% : {'▋'*int(i/2):50s}")


# 处理文件 [s_idx, e_idx)
def zip_file_list(file_list: list[tuple[str, datetime]], s_idx: int, e_idx: int):
    _, zip_name = file_list[s_idx]
    zip_name = f"{zip_name.year}-{zip_name.month}-archive"
    zip_name = f"{zip_name}_{uuid.uuid4()}"
    zip_path = path.join(target_path, zip_name)
    ## 创建压缩包
    with ZipFile(zip_path, "x") as zip:
        for idx in range(s_idx, e_idx):
            file_path, _ = file_list[idx]
            zip.write(file_path)
            # 添加删除操作
            os.remove(file_path)
            i = (idx - s_idx) * 100 / (e_idx - s_idx)
            logger.info(f"{zip_name[0:20]}: {i:2.1f}% : {'▋'*int(i/2):50s}")
    logger.info(f"already done {zip_name} at {zip_path}")
    return


def task(dir_path: str):
    file_list = get_file_list(dir_path)
    group_file_list(file_list)


if __name__ == "__main__":

    logger.info(f"process root dir: {root_path}")
    logger.info(f"move to target dir: {target_path}")
    dir_list = get_root_dir_list(root_path)
    for dir_path in dir_list:
        logger.info(f"handle files at path: {dir_path}")
        file_list = get_file_list(dir_path)
        group_file_list(file_list)

    while 1:
        copy = future_set.copy()
        for fut in copy:
            fut.result()
            if fut.done():
                future_set.remove(fut)
        if len(future_set) == 0:
            break
