from tqdm import tqdm
from multiprocessing import Process, Pool
from glob import glob
import zipfile
import tarfile
import shutil
import os
import argparse
import chardet


def get_args_parser():
    parser = argparse.ArgumentParser('Unzip AIHub Data', add_help=False)
    parser.add_argument('--path', default="/home/smyun/nas4/AIhub/음식 이미지 및 영양정보 텍스트/Validation", type=str,
                        help='Input DIR')
    parser.add_argument('--overwrite', default=False, type=bool,
                        help='overwirte files')
    return parser


def func_unzip(path_to_zip_file, target_path, overwrite_duplication):
    while len(path_to_zip_file) > 0:
        for file in path_to_zip_file:
            print('start', os.path.splitext(file)[0])
            with zipfile.ZipFile(file) as zf:
                # for member in tqdm(zf.infolist(), desc=os.path.basename(file)+' '):
                for member in zf.infolist():
                    try:
                        member.filename = member.filename.encode('cp437').decode('euc-kr', 'ignore')
                    except :
                        print(f'{member.filename} is not encoded with cp437')
                    finally:
                        try:
                            if overwrite_duplication:
                                zf.extract(member, os.path.splitext(file)[0])
                            else: 
                                if not os.path.exists(os.path.join(os.path.splitext(file)[0], member.filename)):
                                    zf.extract(member, os.path.splitext(file)[0])
                        except zipfile.error as e:
                            pass
            os.makedirs(os.path.dirname(file).replace('AIhub', 'AIhub_ZIP_complete') ,exist_ok=True)
            shutil.move(file, file.replace('AIhub', 'AIhub_ZIP_complete'))
            print('Done', os.path.splitext(file)[0])
        path_to_zip_file = glob(os.path.join(target_path, '**/*.zip'), recursive=True)


def list_chunk(lst, n):
    output = [[] for x in range(n)]
    cnt = 0
    for el in lst:
        output[cnt % n].append(el)
        cnt += 1
    return output


def run_multi_proc(path_to_zip_file_chunk, target_path, overwrite_duplication):
    # func_unzip(path_to_zip_file_chunk[0], target_path, overwrite_duplication)
    
    p0 = Process(target=func_unzip, args=(path_to_zip_file_chunk[0], target_path, overwrite_duplication))
    p1 = Process(target=func_unzip, args=(path_to_zip_file_chunk[1], target_path, overwrite_duplication))
    p2 = Process(target=func_unzip, args=(path_to_zip_file_chunk[2], target_path, overwrite_duplication))
    p3 = Process(target=func_unzip, args=(path_to_zip_file_chunk[3], target_path, overwrite_duplication))
    p4 = Process(target=func_unzip, args=(path_to_zip_file_chunk[4], target_path, overwrite_duplication))
    p5 = Process(target=func_unzip, args=(path_to_zip_file_chunk[5], target_path, overwrite_duplication))
    p6 = Process(target=func_unzip, args=(path_to_zip_file_chunk[6], target_path, overwrite_duplication))
    p7 = Process(target=func_unzip, args=(path_to_zip_file_chunk[7], target_path, overwrite_duplication))
    p8 = Process(target=func_unzip, args=(path_to_zip_file_chunk[8], target_path, overwrite_duplication))
    p9 = Process(target=func_unzip, args=(path_to_zip_file_chunk[9], target_path, overwrite_duplication))
    
    p0.start()
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    
    p0.join()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()


def main(args):
    target_path = args.path
    overwrite_duplication = args.overwrite

    path_to_zip_file = glob(os.path.join(target_path, '**/*.zip'), recursive=True)
    path_to_zip_file_chunk = list_chunk(path_to_zip_file, 10)

    run_multi_proc(path_to_zip_file_chunk, target_path, overwrite_duplication)


if __name__ == '__main__' :
    parser = argparse.ArgumentParser('Unzip AIHub Data', parents=[get_args_parser()])
    args = parser.parse_args()
    main(args)
    print("Filnish........................................................")