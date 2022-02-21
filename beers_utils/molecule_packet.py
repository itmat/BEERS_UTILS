from beers_utils.sample import Sample
from beers_utils.molecule import Molecule
import re
import os
import pathlib
import resource
import collections
import pandas

class MoleculePacket:

    next_molecule_packet_id = 0  # Static variable for creating increasing molecule packet id's

    def __init__(self, molecule_packet_id, sample, molecules):
        self.molecule_packet_id = molecule_packet_id
        self.sample = sample
        self.molecules = molecules

    def serialize(self, file_path):
        with open(file_path, 'wb') as obj_file:
            obj_file.write((f"#{self.molecule_packet_id}\n#{self.sample.serialize()}\n").encode(encoding="ascii"))
            for molecule in self.molecules:
                obj_file.write((molecule.serialize() + "\n").encode(encoding="ascii"))

    @staticmethod
    def deserialize(file_path):
        molecules = []
        with open(file_path, 'rb') as obj_file:
            for line_number, line in enumerate(obj_file):
                line = line.rstrip(b"\n")
                if line_number == 0:
                    molecule_packet_id = int(line[1:].decode(encoding="ascii"))
                elif line_number == 1:
                    sample = Sample.deserialize(line.decode(encoding="ascii"))
                else:
                    molecules.append(Molecule.deserialize(line.decode(encoding="ascii")))
        return MoleculePacket(molecule_packet_id, sample, molecules)

    @staticmethod
    def from_CAMPAREE_molecule_file(file_path, packet_id):
        """ Load a CAMPAREE text molecule file as input

        CAMPAREE does not assign sample names and molecule packet ids
        So we assign them based off the filename
        """
        file_path = pathlib.Path(file_path)
        sample_name = file_path.parent.name
        sample_id = sample_name
        sample = Sample(sample_id, sample_name, '', '', False) #TODO: Are these the right extra parameters? Do we care about any of them?

        # load the molecules from the file
        with file_path.open("r") as data:
            molecules = []
            for line in data:
                if line.startswith("#"):
                    continue
                transcript_id, chrom, parental_start, parental_cigar, ref_start, ref_cigar, strand, sequence = line.strip().split("\t")
                mol = Molecule(
                        Molecule.new_id(transcript_id),
                        sequence,
                        start = int(parental_start), # The 'parent molecule' for an RNA transcript is the true ('parental') genome
                        cigar = parental_cigar,
                        strand = strand,
                        transcript_id = transcript_id, # TODO is this right?
                        source_start = int(ref_start), # Source alignment is relative to reference genome
                        source_cigar = ref_cigar,
                        source_strand = strand,
                        source_chrom = chrom)
                molecules.append(mol)
        mol_packet = MoleculePacket(packet_id, sample, molecules)
        return mol_packet

    @staticmethod
    def new_id():
        ID = next_molecule_packet_id
        next_molecule_packet_id += 1
        return ID

    def write_quantification_file(self, file_path):
        """ Write out a file of quantified values of transcript-level expression

        :param file_path: Path to the quantification file

        Output is a tab-separated file containing transcript IDs and transcript counts.
        Transcripts with zero expression are omitted.
        Useful for summarizing generated transcripts without needing to store all files to disk,
        for example when generating packets on the fly for BEERS2.
        """

        quantifications = collections.Counter(mol.transcript_id for mol in self.molecules)
        quant_series = pandas.Series(quantifications, name = "counts").sort_index()
        quant_series.index.name = "transcript_id"
        quant_series.to_csv(file_path, sep="\t")
