'''
Cigar string manipulating utility

From SAM file format description:
                                                              Consumes  Consumes  
 Code Description                                               Query   Reference
 --------------------------------------------------------------------------------
    M alignment match (can be a sequence match or mismatch)     yes     yes
    I insertion to the reference 	                            yes     no
    D deletion from the reference                               no 	    yes
    N skipped region from the reference                         no 	    yes
    S soft clipping (clipped sequences present in SEQ) 	        yes     no
    H hard clipping (clipped sequences NOT present in SEQ)      no      no
    P padding (silent deletion from padded reference)           no      no
    = sequence match 	                                        yes 	yes
    X sequence mismatch 	                                    yes     yes
 --------------------------------------------------------------------------------

 Codes consume query if you advance by the specified number in the query sequence
 They consume reference if you advance by the specified number in the reference sequence

Important function is 'chain_cigar' which applies one cigar ontop of another.
'''

import re
from beers_utils.general_utils import GeneralUtils

consumes = {
    "M": {"query": True,  "ref": True},
    "I": {"query": True,  "ref": False},
    "D": {"query": False, "ref": True},
    "N": {"query": False, "ref": True},
    "S": {"query": True,  "ref": False},
    "H": {"query": False, "ref": False},
    "P": {"query": False, "ref": False},
    "=": {"query": True,  "ref": True},
    "X": {"query": True,  "ref": True},
}

cigar_section = re.compile(r"(\d+)([MIDNSHPX])")
def split_cigar(cigar):
    '''
    Given cigar string, get a list of operations and lengths
    '''
    if cigar == '=':
        return ('=', None) # Matches everything

    matches = re.findall(cigar_section, cigar)
    return [(op, int(num)) for num, op in matches]

def unsplit_cigar(split):
    '''
    Given split cigar like [(op, num), (op, num),...]
    construct a cigar string

    Drops empty and joins like pieces
    '''
    pieces = []
    last_op = None
    for op, num in split:
        if num == 0:
            continue
        if last_op == op:
            pieces[-1] = (op, pieces[-1][1] + num)
        else:
            pieces.append((op, num))
            last_op = op
    return ''.join(str(num)+op for op, num in pieces)

def query_seq_length(split):
    ''' Given split cigar, return length of the query sequence

    NOTE: reference length is not specified in cigar string
    '''
    length = 0
    for op, num in split:
        if consumes[op]['query']:
            length += num
    return length

def match_seq_length(split):
    ''' Given split cigar, return number of bases consumed on reference

    NOTE: reference length is not specified in cigar string
    '''
    length = 0
    for op, num in split:
        if consumes[op]['ref']:
            length += num
    return length

def chain(start1, cigar1, strand1, start2, cigar2, strand2):
    '''
    Given start1, cigar1, strand1 aligning A to B
    and start2, cigar2, strand2 aligning B to C,
    generate start, cigar, strand aligning A to C

    NOTE: if start1, cigar1 imply a longer alignment than the middle query B
    can support, then this will crash. I.e. must truly have alignements from
    A to B to C and not just random cigar strings
    '''

    assert start1 > 0
    assert start2 > 0


    # Index positions (1-based) on each of the three sequences
    query = 1 # Aka 'A' sequence
    middle = 1 # Aka 'B' sequence
    ref = start2 # Aka 'C' sequence

    # TODO: do we want to use a deque instead of a list for these?
    split1 = split_cigar(cigar1)
    split2 = split_cigar(cigar2)

    match_length = match_seq_length(split1)
    middle_length = query_seq_length(split2)
    if strand2 == '-':
        split1 = split1[::-1]
        start1 = middle_length - start1 - match_length + 2

    def advance_to(target):
        ''' advance along the middle query sequence to a target position

        Returns the distance along the reference sequence that was traversed
        and the operations of the cigar strings advanced through in the query
        '''
        nonlocal middle, ref
        skipped = 0
        ops_used = []
        # Advance until middle == target:
        while middle < target:
            op2, num2 = split2.pop(0)
            if consumes[op2]['query']:
                if num2 + middle > target:
                    # Truncate to not go past the target
                    remaining = num2 + middle - target
                    num2 = target - middle
                    split2.insert(0, (op2, remaining))
                middle += num2
            if consumes[op2]['ref']:
                ref += num2
                skipped += num2
            ops_used.append( (op2, num2) )
        return ops_used, skipped

    advance_to(start1)
    assert middle == start1

    # Compile the results
    result = []
    result_start = ref

    # now advance through each step of operations in cigar1
    # recording where they fall on the reference
    for op1, num1 in split1:
        if consumes[op1]['query']:
            query += num1
            if consumes[op1]['ref']: 
                # Figure out all the operations in cigar2 that happen between
                # the start and end of this operation
                target = middle + num1
                ops_used, skipped = advance_to(target)
                for op2, num2 in ops_used:
                    result.append((op2, num2))
            else: #Doesn't consume ref
                result.append((op1, num1))
        else: # cigar1 doesn't consume query
            if consumes[op1]['ref']:
                target = middle + num1
                ops_used, skipped = advance_to(target)
                # Output the distance in ref skipped
                result.append((op1, skipped))
            else:
                # We don't really use these ops (like H or P)
                # and consuming neither at all kind of doesn't make sense
                raise NotImplementedError(f"Cannot handle cigar code {op1}")

    result_cigar = unsplit_cigar(result)
    result_strand = "+" if strand1 == strand2 else '-'

    return result_start, result_cigar, result_strand

def query_from_alignment(start, cigar, strand, reference):
    '''
    Given start position, cigar string and reference sequence,
    we pull out the query sequence from the alignment

    Any insertions (or padding, etc.) is filled as Ns
    '''

    idx = start - 1
    pieces = []
    for op, num in split_cigar(cigar):
        if consumes[op]['query']:
            if consumes[op]['ref']:
                # Match
                pieces.append(reference[idx:idx+num])
                idx += num
            else:
                # Insertion
                pieces.append("N"*num)
        else:
            if consumes[op]['ref']:
                # Deletion, get nothing
                idx += num
            else:
                raise NotImplementedError(f"Cannot handle cigar code {op}")
    seq = ''.join(pieces)
    if strand == "-":
        seq = GeneralUtils.create_complement_strand(seq)
    return seq

if __name__ == '__main__':
    import numpy
    numpy.random.seed(0)
    reference = ''.join(numpy.random.choice(list("ACGT"), size=1000))
    #Test chain() on many random sequences
    for i in range(10000):
        start1 = numpy.random.randint(1,20)
        cigar1 = ''.join(numpy.random.choice(["5M", "6I", "10M", "2D", "5S", "12N"], size=100))
        strand1 = str(numpy.random.choice(["+", "-"]))
        query1 = query_from_alignment(start1, cigar1, strand1, reference)

        if len(query1) > 100:
            start2 = numpy.random.randint(1,20)
            cigar2 = ''.join(numpy.random.choice(['3M', '2I', '15M', '1D', '15N', '3S'], size=5))
            strand2 = str(numpy.random.choice(['+', '-']))
            query2 = query_from_alignment(start2, cigar2, strand2, query1)

            start2_to_ref, cigar2_to_ref, strand2_to_ref = chain(start2, cigar2, strand2, start1, cigar1, strand1)
            query2_from_ref = query_from_alignment(start2_to_ref, cigar2_to_ref, strand2_to_ref, reference)
            match = query2_from_ref == query2

            if not match:
                print(f"Failed!")
                print("Query 1")
                print(start1, cigar1, query1, strand1)
                print("Query 2")
                print(start2, cigar2, query2)
                print("From ref")
                print(start2_to_ref, cigar2_to_ref, strand1, query2_from_ref)
                break
    print("Done testing")
