import re
import sys
import numpy as np

import beers_utils.cigar


class Molecule:
    ''' Represents a molecule of RNA or DNA

    NOTE: start positions are all 1-based, as are all indexes for provided functions

    start, cigar give the alignment relative to the 'parent' molecule,
    which is whatever molecule it came from (e.g. a fragment's parent is the whole molecule)

    source_start, source_cigar give the alignemnt relative to a 'source molecule', which
    is generally useful to use as the reference genome. Unlike start, cigar, operations
    on the molecule should update the source_start and source_cigar to maintain it relative
    to the same source molecule (reference geneome), while start and cigar are always relative
    to the prior step and so only contain information from the most recent operation.

    '''


    next_molecule_id = 1 # Static variable for creating increasing molecule id's
    header = "#molecule_id\ttranscript_id\tsequence\tstart\tcigar\tsource_start\tsource_cigar\tsource_strand\tsource_chrom\tnote\n"
    disallowed = re.compile((r'[^AGTCN]'))
    # TODO: we are currently allowing 'N' as a base, but should probably not in the future

    def __init__(self, molecule_id, sequence, start=None, cigar=None, strand='.',
                 transcript_id=None, source_start=None, source_cigar=None, source_strand='.',
                 source_chrom=''):
        assert (start) > 0
        assert (source_start or 1) > 0
        self.molecule_id = molecule_id
        self.transcript_id = transcript_id

        self.sequence = sequence.strip()

        # Cigar string relative to the 'parent' molecule
        #i .e. the previous step this molecule went through
        # not relative to genome
        self.start = start # NOTE: 1 based
        self.cigar = cigar

        # Alignment relative to a source genome
        self.source_start = source_start or start # NOTE: 1 based
        self.source_cigar = source_cigar or cigar
        self.source_strand = source_strand
        self.source_chrom = source_chrom

    def validate(self):
        match =  Molecule.disallowed.search(self.sequence)
        if match:
            print(f"The molecule having an id of {self.molecule_id} has a disallowed base '{match.group()}'.", file=sys.stderr)
            return False
        return True


    def poly_a_tail_length(self):
        ''' Return length of the PolyA tail (or 0 if none '''
        match = re.search(r'(A+$)', self.sequence)
        return 0 if not match else len(match.group())

    def longest_poly_a_stretch(self):
        ''' Return length of the longest stretch of A bases (or 0 if no As)'''
        match = re.findall(r'(A+)', self.sequence)
        return max((len(m) in match), default=0)

    def substitute(self, nucleotide, position):
        ''' Substitue a single base in the molecule

        nucleotide: new base to substitute in
        position: 1-based position to substitute
        '''
        original_length = len(self.sequence)
        assert 1 <= position <= original_length, "Position must be along the molecule (1-based)."
        idx = position - 1
        self.sequence = self.sequence[:idx] + nucleotide + self.sequence[idx + 1:]
        # Doesn't change cigars or start positions

    def insert(self, insertion_sequence, position):
        ''' Insert a sequence before a given position

        If position == 1, then prepends it to the left (5') end
        if position == len(self.sequence)+1 then append to the right (3') end
        '''
        original_length = len(self.sequence)
        insertion_length = len(insertion_sequence)
        assert 1 <= position <= original_length+1, "Position must be along the molecule."
        idx = position - 1
        if idx == 0:
            self.cigar = f"{insertion_length}I{original_length}M"
            self.sequence = insertion_sequence + self.sequence
        elif idx == original_length:
            self.cigar = f"{original_length}M{insertion_length}I"
            self.sequence = self.sequence + insertion_sequence
        else:
            head = self.sequence[:idx + 1]
            tail = self.sequence[idx + 1:]
            self.cigar = f"{len(head)}M{insertion_length}I{len(tail)}M"
            self.sequence = head + insertion_sequence + tail
        new_source_start, new_source_cigar, new_source_strand = beers_utils.cigar.chain(
                self.start, self.cigar, "+",
                self.source_start, self.source_cigar, self.source_strand,
            )
        self.source_start = new_source_start
        self.source_cigar = new_source_cigar
        self.source_strand = new_source_strand

    def delete(self, deletion_length, position):
        ''' Delete bases starting at position (1-based) '''
        # Position after which to delete
        original_length = len(self.sequence)
        idx = position - 1
        assert 1 <= position <= original_length, "Position must be along the molecule."
        assert idx + deletion_length <= original_length, "Cannot delete past the end of a molecule"
        assert original_length - deletion_length > 0, "Cannot delete the entire molecule in this way."
        if idx == 0:
            self.sequence = self.sequence[deletion_length:]
            self.cigar = f"{deletion_length}D{len(self.sequence)}M"
        elif idx + deletion_length == original_length:
            self.sequence = self.sequence[:original_length - deletion_length]
            self.cigar = f"{len(self.sequence)}M{deletion_length}D"
        else:
            head = self.sequence[:position]
            tail = self.sequence[position + deletion_length:]
            self.cigar = f"{len(head)}M{deletion_length}D{len(tail)}M"
            self.sequence = head + tail
        new_source_start, new_source_cigar, new_source_strand = beers_utils.cigar.chain(
                self.start, self.cigar, "+",
                self.source_start, self.source_cigar, self.source_strand,
            )
        self.source_start = new_source_start
        self.source_cigar = new_source_cigar
        self.source_strand = new_source_strand

    def truncate(self, position):
        ''' Truncate (break) a molecule at a given position
        NOTE: Molecule retains the right (3') end, so truncation happens from left (5') end

        position: 1-based index at which to break (base at this position is retained)
        '''
        # Position after which to break the molecule (0 indexed)
        # For the present, assume that the 3 prime end is always the end retained.
        assert position > 0
        self.start = position
        self.sequence = self.sequence[position - 1:]
        assert len(self.sequence) > 0, "A molecule truncation must leave behind a molecule with non-zero length"
        self.cigar = f"{len(self.sequence)}M"
        new_source_start, new_source_cigar, new_source_strand = beers_utils.cigar.chain(
                self.start, self.cigar, "+",
                self.source_start, self.source_cigar, self.source_strand,
            )
        self.source_start = new_source_start
        self.source_cigar = new_source_cigar
        self.source_strand = new_source_strand

    def make_fragment(self, start,end):
        """ Return a smaller molecule from this molecule

        start: 1-based index of the first base included in fragment
        end: 1-based index of the last base included in the fragment"""
        assert 1 <= start < end <= len(self.sequence)

        frag_sequence = self.sequence[start-1:end]
        frag_length = len(frag_sequence)
        frag_cigar = f"{frag_length}M" # Fragments match their parents
        frag_id = Molecule.new_id(self.molecule_id)
        try:
            new_source_start, new_source_cigar, new_source_strand = beers_utils.cigar.chain(
                    start, frag_cigar, "+",
                    self.source_start, self.source_cigar, self.source_strand,
                )
        except AssertionError:
            print(start, end, frag_length, len(self.sequence), frag_cigar)
            raise

        frag = Molecule(frag_id, frag_sequence, start=start, cigar=frag_cigar,
                source_start = new_source_start,
                source_cigar = new_source_cigar,
                source_strand = self.source_strand,
                source_chrom = self.source_chrom,
                transcript_id = self.transcript_id,
                )

        return frag

    def generate_errors(self, substitution_rate, insertion_rate, deletion_rate, max_insertion_length=3, max_deletion_length=3):
        ''' Perform random 'errors' of the RNA, single nucleotide substitutions, insertions
        and deletions

        :param substituion_rate: rate (per base) of inserting a random base (possibly matching the original)
        :param insertion_rate: rate (per base) of inserting a random string of bases
        :param deletion_rate: rate (per base) of performing a deletion
        :param max_insertion_length: max length of insertion, uniformly chosen from 1 to this value
        :param max_deletion_length: max length of deletion, uniformly chosen from 1 to this value

        Note that deletions or insertions may occur adjacent to each other and thereby
        become longer than the specified max length
        '''

        substitutions = np.random.random(len(self.sequence)) <= substitution_rate
        for idx in np.nonzero(substitutions)[0]:
            random_base = np.random.choice(list("ACGT"))
            self.substitute(random_base, idx + 1)

        if deletion_rate > 0:
            # Iteratively advance to the next deletion site, perform the deletion, repeat
            # Since length of sequence is constantly changing, we don't just compute
            # the deletion sites in advance like in the substitution case
            idx = np.random.geometric(deletion_rate) # note: automatically 1-based
            while idx < len(self.sequence):
                # Perform a deletion
                deletion_length = np.random.randint(1, max_deletion_length+1)
                deletion_length = min(deletion_length ,len(self.sequence) - idx)
                self.delete(deletion_length, idx)

                # Move to the next deletion, until past the end of the sequence
                idx += np.random.geometric(deletion_rate)

        if insertion_rate > 0:
            # Do the same for insertions
            idx = np.random.geometric(insertion_rate) # note: automatically 1-based
            while idx < len(self.sequence):
                # Perform a insertion
                insert_size = np.random.randint(1, max_insertion_length)
                insert_seq = ''.join(np.random.choice(list("ACGT"), size=insert_size))
                self.insert(insert_seq, idx)

                # Move to the next insertion, until past the end of the sequence
                idx += np.random.geometric(insertion_rate) + insert_size

    def __len__(self):
        return len(self.sequence)

    def __str__(self):
        return(
            str({"id": self.molecule_id,
                 "sequence": self.sequence,
                 "start": self.start,
                 "cigar": self.cigar,
                 "source_start": self.source_start,
                 "source_cigar": self.source_cigar,
                 "source_strand": self.source_strand,
                 "source_chrom": self.source_strand,
                 "transcript id": self.transcript_id})
        )

    def __repr__(self):
        return f"Molecule({self.molecule_id}, {self.sequence}, {self.start}, {self.cigar}," \
               f" {self.transcript_id}, {self.source_start}, {self.source_cigar})"

    def log_entry(self, note = ''):
        return "\t".join([
                repr(self.molecule_id),
                repr(self.transcript_id),
                self.sequence,
                str(self.start or ''),
                self.cigar or '',
                str(self.source_start or ''),
                self.source_cigar or '',
                self.source_strand or '',
                self.source_chrom or '',
                repr(note),
            ]) + "\n"

    @staticmethod
    def new_id(parent_id=""):
        new_id = Molecule.next_molecule_id
        Molecule.next_molecule_id += 1
        return f"{parent_id}.{new_id}"

    def serialize(self):
        return(f"{self.molecule_id}\t{self.sequence}\t{self.start}\t{self.cigar}\t{self.transcript_id}\t"
               f"{self.source_start}\t{self.source_cigar}\t{self.source_strand}\t{self.source_chrom}")

    @staticmethod
    def deserialize(data):
        data = data[1:] if (data.startswith("#")) else data
        molecule_id, sequence, start, cigar, transcript_id, source_start, source_cigar, source_strand, source_chrom = data.rstrip('\n').split('\t')
        return Molecule(
                molecule_id,
                sequence,
                start = int(start),
                cigar = cigar,
                transcript_id = transcript_id,
                source_start = int(source_start),
                source_cigar = source_cigar,
                source_strand = source_strand,
                source_chrom = source_chrom)

class BeersMoleculeException(Exception):
    """Base class for other molecule exceptions."""
    pass

if __name__ == "__main__":
    source = "AGTTCAAGCTTGCACTCTAG"
    source_length = len(source)
    molecule = Molecule("1", source, start="1", cigar=f"{source_length}M")
    print(molecule)
    molecule.substitute("C", 15)
    print(molecule)
    molecule.insert("AGT", 4)
    print(molecule)
    molecule.delete(2, 2)
    print(molecule)
