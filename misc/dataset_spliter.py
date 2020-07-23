import codecs
import argparse
import os
import random
from tqdm import tqdm


def main(args):
    if len(args.rate) != 3 or sum(args.rate) != 1:
        raise ValueError("illegal rate")
    
    if not os.path.exists(args.dir_out):
        os.makedirs(args.dir_out)
    
    out_dirs = [os.path.join(args.dir_out, sub_dir) for sub_dir in ['train', 'dev', 'test']]
    for out_dir in out_dirs:
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    
    fin = codecs.open(args.file_in, 'r', encoding='utf8')
    fout0 = codecs.open(os.path.join(out_dirs[0], os.path.basename(args.file_in)), 'w', encoding='utf8')
    fout1 = codecs.open(os.path.join(out_dirs[1], os.path.basename(args.file_in)), 'w', encoding='utf8')
    fout2 = codecs.open(os.path.join(out_dirs[2], os.path.basename(args.file_in)), 'w', encoding='utf8')
    
    for line in tqdm(fin):
        rand = random.random()
        if rand < args.rate[0]:
            fout0.write(line)
        elif rand < sum(args.rate[:2]):
            fout1.write(line)
        else:
            fout2.write(line)
    
    fin.close()
    fout0.close()
    fout1.close()
    fout2.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--file_in', type=str)
    parser.add_argument('--dir_out', type=str)
    parser.add_argument('--rate', type=float, nargs='+')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    main(args)
