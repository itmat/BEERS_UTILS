from beers_utils.sample import Sample
from beers_utils.molecule import Molecule
import re
import os
import pathlib
import resource

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
                line = line.rstrip()
                if line_number == 0:
                    molecule_packet_id = int(line[1:].decode(encoding="ascii"))
                elif line_number == 1:
                    sample = Sample.deserialize(line.decode(encoding="ascii"))
                else:
                    molecules.append(Molecule.deserialize(line.decode(encoding="ascii")))
        return MoleculePacket(molecule_packet_id, sample, molecules)

    @staticmethod
    def from_CAMPAREE_molecule_file(file_path):
        """ Load a CAMPAREE text molecule file as input

        CAMPAREE does not assign sample  names and molecule packet ids
        So we must assign those automatically from filenames.
        sample is the folder name,
        molecule packet id is any number in the filename, e.g. 'molecule_file###.txt'"""
        file_path = pathlib.Path(file_path)
        sample_id = file_path.parent.name
        sample = Sample(sample_id, sample_id, '', '', False) #TODO: Are these the right extra parameters? Do we care about any of them?
        packet_id = re.search(r"(\d+)", str(file_path.name))
        if packet_id:
            packet_id = int(packet_id.groups()[0])
        else:
            packet_id = 0 #No number, just give it ID 0

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
                        start = 1, # relative to 'parent molecule', which is itself (one-based)
                        cigar = f"{len(sequence)}M", # relative to 'parent molecule', which is itself
                        strand = '+',
                        transcript_id = transcript_id, # TODO is this right?
                        source_start=int(ref_start), # Source alignment is relative to reference genome
                        source_cigar=ref_cigar,
                        source_strand=strand,
                        source_chrom=chrom)
                molecules.append(mol)
        mol_packet = MoleculePacket(packet_id, sample, molecules)
        return mol_packet

    @staticmethod
    def new_id():
        ID = next_molecule_packet_id
        next_molecule_packet_id += 1
        return ID
