from src.results import process_file as pf
from flair.models import SequenceTagger


if __name__ == '__main__':
    path = "/Users/thomasclavier/Documents/Projects/Etalab/prod/pseudo_conseil_etat/src/database/IN/DCA/CAA13/2013/20130215/10MA01447.doc"
    tagger = SequenceTagger.load('/Users/thomasclavier/Documents/Projects/Etalab/prod/pseudo_conseil_etat/src/models/models/best-model.pt')


    pf.main(path, tagger)
