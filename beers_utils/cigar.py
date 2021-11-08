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
'''

import re

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

def chain(start1, cigar1, start2, cigar2):
    '''
    Given start1, cigar1 aligning A to B and start2, cigar2 aligning B to C,
    generate start, cigar aligning A to C

    NOTE: if start1, cigar1 imply a longer alignment than the middle query B
    can support, then this will crash. I.e. must truly have alignements from
    A to B to C and not just random cigar strings
    '''

    assert start1 > 0
    assert start2 > 0

    # TODO: do we want to use a deque instead of a list for these?
    split1 = split_cigar(cigar1)
    split2 = split_cigar(cigar2)

    # Index positions (1-based) relative to the reference
    query = 1
    middle = 1
    ref = start2

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
    return result_start, result_cigar

def query_from_alignment(start, cigar, reference):
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
    return ''.join(pieces)

if __name__ == '__main__':
    import numpy
    numpy.random.seed(0)
    reference = ''.join(numpy.random.choice(list("ACGT"), size=1000))
    #Test chain() on many random sequences
    for i in range(10000):
        start1 = numpy.random.randint(1,20)
        cigar1 = ''.join(numpy.random.choice(["5M", "6I", "10M", "2D", "5S", "12N"], size=100))
        query1 = query_from_alignment(start1, cigar1, reference)

        if len(query1) > 100:
            start2 = numpy.random.randint(1,20)
            cigar2 = ''.join(numpy.random.choice(['3M', '2I', '15M', '1D', '15N', '3S'], size=5))
            query2 = query_from_alignment(start2, cigar2, query1)

            start2_to_ref, cigar2_to_ref = chain(start2, cigar2, start1, cigar1)
            query2_from_ref = query_from_alignment(start2_to_ref, cigar2_to_ref, reference)
            match = query2_from_ref == query2

            if not match:
                print(f"Failed!")
                print("Query 1")
                print(start1, cigar1, query1)
                print("Query 2")
                print(start2, cigar2, query2)
                print("From ref")
                print(start2_to_ref, cigar2_to_ref, query2_from_ref)
                break
    print("Done testing")
