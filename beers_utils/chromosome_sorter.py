import sys
import os
import re
import argparse
import roman
import functools


class ChromosomeSort:
    """
    Provides a facility for sorting a list of chromosome names by means of a ChromosomeName class.
    The ordering is as follows:
    1.  leading digits (sorted numerically)
    2.  leading roman numerals (sorted by arabic number equivalents)
    3.  leading names potentially identified as gender related chromosomes (sorted x,y,m)
    4.  leading 'chr'
    5.  leading alphabetic characters (sorted by dictionary order)
    6.  leading non-alphanumeric characters (sorted by ascii order)

    Case is disregarded.  For any of the items above, in the event of equivalence of these leading characters, any
    following character strings that start with a character different from the leading character classification,
    are used to create new ChromosomeName classes and then again compared - essentially recursing through the string
    representing the ChromosomeName.
    """

    def __init__(self, chromosome_name_filename):
        """
        Holds the file containing the list of chromosome names.
        :param chromosome_name_filename:
        """
        self.chromosome_name_filename = chromosome_name_filename

    def sort_chromosome_names(self):
        """
        Sorts the chromosome names provided according to the rules stated in the class documentation.  Both the
        unsorted and sorted list are printed.
        """

        # Populate a list with the chromosome names (1 per line) from the provided file.  Each chromosome name is
        # used to instantiate a ChromosomeName object, which is capable or making comparisons.
        chromosome_names = []
        with open(self.chromosome_name_filename, 'r+') as chromosome_name_file:
            for line in chromosome_name_file:
                chromosome_names.append(ChromosomeName(line.rstrip('\n')))

        # Print the original list, sort and print the sorted list.
        print('\n'.join([chromosome_name.original_content for chromosome_name in chromosome_names]))
        print('---')
        results = sorted(chromosome_names)
        print('\n'.join([chromosome_name.original_content for chromosome_name in results]))

    @staticmethod
    def sort_chromosome_name_list(listing):
        chromosome_names = [ChromosomeName(item) for item in listing]
        results = sorted(chromosome_names)
        return [chromosome_name.original_content for chromosome_name in results]

    @staticmethod
    def sort_file_by_chromosome_coordinates(input_filename, chrom_column, start_column=None, end_column=None,
                                            header=True, sorted_filename=None):
        """Sorts a tab-delimited file by chromosomal coordinates.

        Note, this function will overwrite the contents of any existing sorted
        file matching the default or given filename.

        Parameters
        ----------
        input_filename : string
            Path to file that will be sorted.
        chrom_column : int
            Column number in the input file containing chromosome name. Note,
            column numbers are base=1.
        start_column : int
            [Optional] Column number in input file containing start coordinate
            of chromosome span. If not given, file sorted as if all start
            coordinates are 0.
        end_column : int
            [Optional] Column number in input file containing end coordinate of
            chromosome span. If not given, file sorted as if all end coordinates
            are 0.
        header : Boolean
            [Optional] First line in the input file contains a header. If true,
            first line from input file is copied directly as first line in
            sorted file (not included in sorting). If false, first line is
            treated as any other in the file and included in the sorting.
            Default: True.
        sorted_filename : string
            [Optional] Path to sorted file. If none given, it will use the
            original filename with ".sorted" inserted before the extension.

        """

        if sorted_filename is None:
            sorted_filename = os.path.splitext(input_filename)[0] + ".sorted" + os.path.splitext(input_filename)[1]

        #Convert base=1 column references to base=0 for quick access to columns
        #once they've been read into a python list.
        chrom_column -= 1
        if start_column is not None:
            start_column -= 1
        if end_column is not None:
            end_column -= 1

        #Clear pre-existing sorted file.
        try:
            os.remove(sorted_filename)
        except OSError:
            pass

        with open(input_filename, 'r') as input_file, \
                open(sorted_filename, 'w') as sorted_file:

            # List of ChromosomeCoordinate objects. Once all coordinates from
            # input file loaded, this list will be used to sort everything in
            # order by chromosome coordinate.
            unsorted_chrom_coordinates = []

            # Maps a string of chromosome coordinates to corresponding entries
            # from the input file.
            # Key = String representation of ChromosomeCoordinate
            #       ("{chr}\t{start}\t{end}")
            # Value = newline-delimited string of all lines from the input file
            #        mapping to corresponding coordinates.
            coordinates_to_entries = {}

            # Copy header from input file
            if header is True:
                line = input_file.readline()
                sorted_file.write(line)

            # Load all entries from input file into memory, in preparation for sort.
            for line in input_file:
                line_data = line.split('\t')
                chrom_name = line_data[chrom_column]
                start_coord = 0
                end_coord = 0
                if start_column is not None:
                    start_coord = int(line_data[start_column])
                if end_column is not None:
                    end_coord = int(line_data[end_column])

                current_coord = ChromosomeCoordinate(chrom_name, start_coord, end_coord)
                #Use string form of ChromosomeCoordinate as key ("{chr}\t{start}\t{end}").
                if str(current_coord) not in coordinates_to_entries:
                    unsorted_chrom_coordinates.append(current_coord)
                    coordinates_to_entries[str(current_coord)] = line
                else:
                    #If matching chromosome span already encountered, append
                    #new feature's information to the running list of entries.
                    coordinates_to_entries[str(current_coord)] += line


            #Sort chromosome coordinates, then save contents of input file to
            #output file in sorted order.
            sorted_chrom_coordinates = sorted(unsorted_chrom_coordinates)
            for coord in sorted_chrom_coordinates:
                #Use string form of ChromosomeCoordinate as key ("{chr}\t{start}\t{end}").
                sorted_file.write(coordinates_to_entries[str(coord)])


    @staticmethod
    def main():
        """
        Entry point into the chromosome sorter.  Parses the argument list, which presently includes just the
        path of the chromosome list file and invokes the sorter.
        """
        parser = argparse.ArgumentParser(description='Sort Chromosome Names')
        parser.add_argument('-c', '--chromosome_name_filename',
                            help="Path to file containing one chromosome name per line of the chromosome"
                                 " to be sorted.")
        args = parser.parse_args()
        print(args)
        chromosome_sort = ChromosomeSort(args.chromosome_name_filename)
        chromosome_sort.sort_chromosome_names()


@functools.total_ordering
class ChromosomeName:
    """
    Class holds the chromosome name and provides methods for making comparisons.  Case is disregarded.
    """

    def __init__(self, content):
        """
        Provide a lower case version of the string representation of the chromosome name.
        :param content: string representation of the chromosome name
        """
        self.original_content = content
        self.content = content.lower()

    def __eq__(self, other):
        """
        Test equivalence.  Trivial comparison of the ChromosomeName string with that of another.  None
        ChromosomeName objects are never equivalent.
        :param other: other ChromosomeName object for comparison
        :return: True if equivalent and false otherwise.
        """
        return isinstance(other, ChromosomeName) and self.content == other.content

    def __gt__(self, other):
        """
        Where the real comparison work is done.  Makes comparison between this ChomosomeName and the one
        provided based upon the ordering rules described in the ChomosomeSort class documentation.
        :param other: other ChromosomeName object for comparison
        :return: True if this ChromosomeName is greater and false otherwise.
        """

        # Remaining content > no content.  In recursively created ChromosomeName objects from the trailing content,
        # eventually we create ChromosomeName objects that are empty of content.  For the case where one name is a
        # substring of the other, put the shorter name first.
        if not self.content:
            return False
        if not other.content:
            return True

        # Names starting with digits > names not starting with digits
        if not self.content[0].isdigit() and other.content[0].isdigit():
            return True
        if self.content[0].isdigit() and not other.content[0].isdigit():
            return False

        # Extract leading digits, if any, and any trailing content not starting with a digit
        leading_digit_pattern = '^(\d+)(.*)$'
        name_match = re.match(leading_digit_pattern, self.content)
        other_name_match = re.match(leading_digit_pattern, other.content)

        # If both names have leading numerical content, compare them (as numbers)
        if name_match and other_name_match:
            if int(name_match.group(1)) > int(other_name_match.group(1)):
                return True
            if int(name_match.group(1)) < int(other_name_match.group(1)):
                return False

            # if the digits match, check whether one chromosome name object created from the remaining string is
            # greater than the other chromosome name object.
            return ChromosomeName(name_match.group(2)) > ChromosomeName(other_name_match.group(2))

        # Names starting with roman numerals and special XYM gender designators > names not starting with roman
        # numerals or special gender designators
        # Recall that leading digits have already been handled at this point.
        if not re.match('^[ivxym]', self.content) and re.match('^[ivxym]', other.content):
            return True
        if re.match('^[ivxym]', self.content) and not re.match('^[ivxym]', other.content):
            return False

        # Extract leading roman numerals, if any, and any trailing content not starting with a roman numeral.
        leading_roman_pattern = '^([ivx]+)(.*)$'
        name_match = re.match(leading_roman_pattern, self.content)
        other_name_match = re.match(leading_roman_pattern, other.content)

        # If both names have leading roman numeral content, compare them (as arabic number equivalents).
        if name_match and other_name_match:
            name_roman = Roman(name_match.group(1))
            other_roman = Roman(other_name_match.group(1))
            if name_roman > other_roman:
                return True
            if name_roman < other_roman:
                return False

            # if the roman numerals match, check whether one chromosome name object created from the remaining string
            # is greater than the other chromosome object.
            return ChromosomeName(name_match.group(2)) > ChromosomeName(other_name_match.group(2))

        # X or Y before M
        if self.content[0] == 'm' and (other.content[0] == 'x' or other.content[0] == 'y'):
            return True
        if (self.content[0] == 'x' or self.content[0] == 'y') and other.content[0] == 'm':
            return False

        # Names not starting with 'chr' > names starting with 'chr'
        # Recall that leading digits, gender designators, and roman numerals have already been handled at this point.
        if not re.match('^chr', self.content) and re.match('^chr', other.content):
            return True
        if re.match('^chr', self.content) and not re.match('^chr', other.content):
            return False

        # Extract leading 'chr' strings, if any, and any trailing content.
        leading_chr_pattern = '^(chr)(.*)$'
        name_match = re.match(leading_chr_pattern, self.content)
        other_name_match = re.match(leading_chr_pattern, other.content)

        # If both names have a leading 'chr' string, only the trailing strings need be compared.
        if name_match and other_name_match:

            # check whether one chromosome name object created from the
            # trailing content is greater than the other chromosome object similarly created.
            return ChromosomeName(name_match.group(2)) > ChromosomeName(other_name_match.group(2))

        # Names not starting with alphabetic characters > names starting with alphabetic characters.
        # Recall that leading digits, gender designators, roman_numerals, and 'chr' have already been handled at
        # this point.
        if not self.content[0].isalpha() and other.content[0].isalpha():
            return True
        if self.content[0].isalpha() and not other.content[0].isalpha():
            return False

        # Extract leading alphabetic content, if any, and any trailing content not starting with an alphabetic character
        leading_alphabetic_pattern = '^([a-z]+)(.*)$'
        name_match = re.match(leading_alphabetic_pattern, self.content)
        other_name_match = re.match(leading_alphabetic_pattern, other.content)

        # If both names have leading alphabetic content, compare them
        if name_match and other_name_match:

            # Which character string is greater is determined by string >
            if name_match.group(1) > other_name_match.group(1):
                return True
            if name_match.group(1) < other_name_match.group(1):
                return False

            # if both alphabetic strings match, check whether one chromosome name object created from the
            # remaining string is greater than the other chromosome object.
            result = ChromosomeName(name_match.group(2)) > ChromosomeName(other_name_match.group(2))
            return result

        # At this point, the leading characters are not digits, roman numerals, special gender related chars or
        # alphabetic characters, leaving just names starting with non-alphanumeric characters.  Since there are no
        # rules for how non-alphanumeric characters are orders we will rely on string comparisons.

        # Extract leading non-alphanumeric content, if any, and any trailing content not starting with a
        # non-alphanumeric character.
        leading_non_alphanumeric_pattern = '^(\W+)(.*)$'
        name_match = re.match(leading_non_alphanumeric_pattern, self.content)
        other_name_match = re.match(leading_non_alphanumeric_pattern, other.content)

        # If both names have leading non-alphanumeric content, compare them
        if name_match and other_name_match:

            # Which character string is greater is determined by string >
            if name_match.group(1) > other_name_match.group(1):
                return True
            if name_match.group(1) < other_name_match.group(1):
                return False

            # If both non-alphanumeric contents match, check whether one chromosome name object created from the
            # trailing content is greater than the other chromosome object similarly created.
            result = ChromosomeName(name_match.group(2)) > ChromosomeName(other_name_match.group(2))
            return result

        # Theoretically, we shouldn't get here
        return True



class ChromosomeCoordinate(ChromosomeName):
    """Class holds chromosome name, start and end coordinates for making comparisons.

    This is a subclass of ChromosomeName, so ChromosomeName comparator functions
    are used for name component of each chromosome, while the coordinates are
    compared numerically.

    Parameters
    ----------
    content : string
        Name of the chromosome compatible with ChromosomeName class.
    start_coord : int
        (Optional) Start coordinate of chromosomal coordinate. Defaults to 0.
    end_coord : int
        (Optional) End coordinate of chromosomal coordinate. Defaults to
        start_coord value.

    Attributes
    ----------
    start_coord - int
    end_coord - int

    """

    def __init__(self, chrom_name, start_coord=None, end_coord=None):
        """
        Provide ChromosomeName representation of chromosome name string.
        """
        ChromosomeName.__init__(self, chrom_name)
        if start_coord is None:
            self.start_coord = 0
        else:
            self.start_coord = int(start_coord)
        if end_coord is None:
            self.end_coord = int(self.start_coord)
        else:
            self.end_coord = int(end_coord)

    def __repr__(self):
        """Return representation of ChromosomeCoordinate object and its contents."""
        return '{class_name}(content="{content}", start_coord={start_coord}, end_coord={end_coord})'.format(
            class_name=self.__class__.__name__,
            content=self.content,
            start_coord=self.start_coord,
            end_coord=self.end_coord
        )

    def __str__(self):
        """Print contents of ChromosomeCoordinate object in "chr\tstart\tend" format."""
        return "{chrom_name}\t{start_coord}\t{end_coord}".format(
            chrom_name=self.content,
            start_coord=self.start_coord,
            end_coord=self.end_coord
        )

    def __eq__(self, other):
        """Test equivalence between this and given ChromosomeCoordinate object.

        ChromosomeNames, start coordinates, and end coordinates must all be
        equal.

        Parameters
        ----------
        other : ChromosomeCoordinate
            Other ChromosomeCoordinate object for comparison

        Returns
        -------
        Boolean
            True if equivalent and false otherwise.

        """
        return isinstance(other, ChromosomeCoordinate) and \
            ChromosomeName.__eq__(self, other) and \
            (self.start_coord, self.end_coord) == (other.start_coord, other.end_coord)

    def __ne__(self, other):
        """Test non-equivalence between this and given ChromosomeCoordinate object.

        Just the negation of __eq__().

        Parameters
        ----------
        other : ChromosomeCoordinate
            Other ChromosomeCoordinate object for comparison

        Returns
        -------
        Boolean
            True if not equivalent and false otherwise.

        """
        return not self.__eq__(other)

    def __gt__(self, other):
        """Test if this is greater than the given ChromosomeCoordinate.

        Makes comparisons in the following order: ChromosomeNames, start
        coordinate, end coordinate.

        Parameters
        ----------
        other : ChromosomeCoordinate
            Other ChromosomeCoordinate object for comparison.

        Returns
        -------
        Boolean
            True if this ChromosomeCoordinate is greater, false otherwise.

        """
        if ChromosomeName.__eq__(self, other):

            if self.start_coord == other.start_coord:
                return self.end_coord > other.end_coord

            return self.start_coord > other.start_coord

        return ChromosomeName.__gt__(self, other)

    def __ge__(self, other):
        """Test if this is greater or equal to the given ChromosomeCoordinate.

        Parameters
        ----------
        other : ChromosomeCoordinate
            Other ChromosomeCoordinate object for comparison.

        Returns
        -------
        Boolean
            True if this ChromosomeCoordinate is greater or equal, false otherwise.

        """
        return self.__gt__(other) or self.__eq__(other)

    def __lt__(self, other):
        """Test if this is less than the given ChromosomeCoordinate.

        Obtained by determining whether this ChromosomeCoordinate is greater or
        equal the given and negating that finding.

        Parameters
        ----------
        other : ChromosomeCoordinate
            Other ChromosomeCoordinate object for comparison.

        Returns
        -------
        Boolean
            True if this ChromosomeCoordinate is less, false otherwise.

        """
        return not self.__gt__(other) and self.__ne__(other)

    def __le__(self, other):
        """Test if this is less or equal to the given ChromosomeCoordinate.

        Obtained by determining whether this ChromosomeCoordinate is greater and
        negating that finding.

        Parameters
        ----------
        other : ChromosomeCoordinate
            Other ChromosomeCoordinate object for comparison.

        Returns
        -------
        Boolean
            True if this ChromosomeCoordinate is less or equal, false otherwise.

        """
        return not self.__gt__(other)


class Roman:
    """
    Class to hold string identified as a roman numeral string, along with its arabic number equivalent.  Comparisons
    are based on a comparison of the associated arabic numbers.
    """

    def __init__(self, content):
        self.content = content
        self.arabic_equivalent = roman.fromRoman(content.upper())

    def __eq__(self, other):
        return isinstance(other, Roman) and self.content == other.content

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return self.arabic_equivalent > other.arabic_equivalent

    def __lt__(self, other):
        return self.arabic_equivalent < other.arabic_equivalent


if __name__ == "__main__":
    sys.exit(ChromosomeSort.main())
