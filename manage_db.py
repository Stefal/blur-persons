#!/usr/bin/env python3
import os
import sys
import argparse
from pathlib import Path
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, update, select
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class Picture(Base):
    __tablename__ = 'picture_list'
    id = Column(Integer, primary_key=True)
    abs_path = Column(String)
    rel_path = Column(String)
    processing = Column(Boolean)
    processing_start_ts = Column(Integer)
    processed = Column(Boolean)
    success = Column(Boolean)

def check_dir(mypath):
    assert Path(mypath).is_dir()
    return mypath

def add_to_db(picture_list):
    added_pic = 0
    dupli_pic = 0
    for pic in picture_list:
        if session.query(Picture).filter_by(rel_path = str(pic.relative_to(Path(args.database).parent))).scalar() is None:
            session.add(Picture(abs_path=str(pic.absolute()), rel_path = str(pic.relative_to(Path(args.database).parent)), processing = False, processed = False))
            session.flush()
            added_pic += 1
        else:
            print("Duplicate: {}".format(str(pic)))
            dupli_pic += 1
    session.commit()
    print("SUMMARY: On {} total pics, {} were added and {} are duplicate".format(len(picture_list), added_pic, dupli_pic))

def parse_arg():
    parser = argparse.ArgumentParser(
        description="Manage pictures database.")
    parser.add_argument("-d", "--database", default=None,
        help="path to the database file")
    parser.add_argument("-c", "--create_db", default=False, action="store_true",
        help="create the database")
    parser.add_argument("-a", "--add_to_db", default=False, action="store_true", help="add pictures to the database")
    parser.add_argument("-p", "--picture_path", type=check_dir, help="path to the picture folder you want to add to the database")
    parser.add_argument("-r", "--recursive", action="store_true",
        help="Add images in subfolder too")
    parser.add_argument("-u", "--unprocessed", action="store_true", help="list unprocessed images")
    args = parser.parse_args()
    print(args)
    return args

if __name__ == '__main__':
    args = parse_arg()
    if args.create_db and not os.path.exists(args.database):
        print("Creating the database...")
        engine = create_engine("sqlite+pysqlite:///" + os.path.abspath(args.database), echo=True, future=True)
    elif args.create_db is False and os.path.exists(args.database):
        print("Loading the database...")
        engine = create_engine("sqlite+pysqlite:///" + os.path.abspath(args.database), echo=True, future=True)
    else:
        print("no database available or database already existing")
        sys.exit()
    session = Session(engine)
    Base.metadata.create_all(engine)
    
    if args.recursive and os.path.isdir(args.picture_path):
        print("Searching for pictures....")
        pic_list = [file for file in Path(args.picture_path).rglob('*.[Jj][Pp][Gg]')]
    elif args.recursive is False and args.picture_path is not None and os.path.isdir(args.picture_path):
        print("Searching for pictures....")
        pic_list = [Path(os.path.join(os.path.abspath(args.picture_path), file)) for file in os.listdir(args.picture_path) if file.lower().endswith(".jpg")]
        #TODO stop this mixup between os.path and Path modules
    
    #print("pic_list: ", pic_list)
   
    if 'pic_list' in locals() and args.add_to_db == True:
        add_to_db(pic_list)
    
    if args.unprocessed == True:
        #result = session.query(Picture).filter(Picture.processing == False, Picture.processed == False)
        result = session.query(Picture).filter(Picture.processing == True, Picture.processed == False).first()
        print("TYPE RESULT: {}".format(type(result)))
        for row in result:
            print(row.rel_path)
        print("Unproccessed pictures: {}".format(result.count()))
    session.close()
