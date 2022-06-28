
def filter(fn):
    lines = []
    with open(fn,'r') as inf:
        for line in inf:
            if 'TIMED_OUT' in line:
                continue
            if line.strip()=='':
                continue
            x = line.strip().split('\t')
            p = float(x[1])
            if p < 0.05:
                continue
            if 'causes' in x[0]:
                continue
            if 'contributes' in x[0]:
                continue
            if 'subclass' in x[0]:
                continue
            rp = x[0].split(':-')
            # current 3 hops have some tautologies in which treats(e0,e1) occurs on the right hand side
            if rp[0] in rp[1]:
                continue
            lines.append( (p, int(x[-1]), line ) )
    return lines

def run(infile,outfile):
    lines = filter(infile)
    lines.sort()
    lines.reverse()
    with open(outfile,'w') as outf:
        for line in lines:
           outf.write(line[-1]) 

def go():
    infiles=['treats_rules_metrics_len_3_weight_100_conf_0.05_depth_2_partial.txt','corrected_robokop2-4_treats_len2_rules.txt']
    outfiles=['3hops_old.txt','2hops_new.txt']
    for infile,outfile in zip(infiles,outfiles):
        run(infile,outfile)

if __name__ == '__main__':
    go()

