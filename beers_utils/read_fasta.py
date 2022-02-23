def read_fasta(fasta_file):
    ''' Read contents of a fasta file in.

    Returns a dictionary mapping sequence names to sequences
    '''
    with open(fasta_file) as fasta:
        line_header = None
        seq = []
        seqs = {}
        for line in fasta:
            line = line.rstrip("\n")
            if line.startswith(">"):
                if line_header != None:
                    seqs[line_header] = ''.join(seq)
                seq = []
                line_header = line.lstrip(">")
            else:
                seq.append(line)
        seqs[line_header] = ''.join(seq)
    return seqs
