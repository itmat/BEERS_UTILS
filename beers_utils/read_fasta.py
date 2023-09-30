import io
import numpy as np

BASES = list(b"ACGT")
BASES_WITH_N = list(b"ACGTN")
N = 78 # ASCII code

def read_fasta(fasta_file, replace_Ns = False, rng = None, strip_chrom_names = True):
    ''' Read contents of a fasta file in.

    :param replace_Ns: if True, use random number generator rng to replace Ns with random ACGT strings
    :param rng: numpy RNG; used only if replace_Ns is True
    :param strip_chrom_names: if True (default), remove everything in the chromosome/contig name after
                                any whitespace

    Returns a dictionary mapping sequence names to sequences
    '''
    with open(fasta_file, "rb") as fasta:
        line_header = None
        seq = []
        seqs = {}
        for line in fasta:
            line = line.rstrip(b"\n")
            if line.startswith(b">"):
                if line_header != None:
                    seqs[line_header] = b''.join(seq).decode()
                seq = []
                line_header = line.lstrip(b">").decode()
                if strip_chrom_names:
                    line_header = line_header.split()[0]
            else:
                encoded = np.frombuffer(line, dtype='uint8')

                if not np.isin(encoded, BASES_WITH_N).all():
                    raise ValueError(f"Invalid characters found in the fasta file {fasta_file}: all must be in ACGTN")

                if replace_Ns:
                    is_N = encoded == N
                    if is_N.any():
                        choices = rng.choice(BASES, size=len(encoded))
                        encoded = encoded.copy()
                        encoded[is_N] = choices[is_N]
                        line = encoded.view(f"S{len(encoded)}")[0]

                seq.append(line)
        seqs[line_header] = b''.join(seq).decode()
    return seqs
