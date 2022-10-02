import re
import json
import argparse

def read_json(input_data) :
    f = open(input_data)
    data = json.load(f)
    return data

def cleaning_data(data) :
    result = {}
    for items in data :
        pre_clean_items = items['komoditas'].replace(",","|").replace("ikan ", "")
        clean_items = pre_clean_items.replace("| ", "|").replace("ikan","").replace(" ","|").split('|')
        pre_clean_berat = items['berat']. replace("rata2", "rata rata")
        clean_berat = re.findall('\d+', pre_clean_berat)
        test_items = []
        for i in range(len(clean_items)) :
            if len(clean_items) == len(clean_berat) :
                if clean_items[i] in result :
                    result[clean_items[i]] += int(clean_berat[i])
                else :
                    result[clean_items[i]] = int(clean_berat[i])
            else :
                if len(clean_berat) > 0 :
                    if "rata" in pre_clean_berat and "kecuali" not in pre_clean_berat :
                        # the cleaniest and correct one of the data
                        if clean_items[i] in result :
                            result[clean_items[i]] += int(clean_berat[0])
                        else :
                            result[clean_items[i]] = int(clean_berat[0])
                    else :
                        # should be have another clausal but for this particular time, I just make it this way
                        if clean_items[i] in result :
                            result[clean_items[i]] += int(clean_berat[0])
                        else :
                            result[clean_items[i]] = int(clean_berat[0])
    return result

def add_weight(data) :
    for key in data :
        data[key] = str(data[key]) + "kg"
    return data

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        help='Input your data (soal-2.json)'
    )

    known_args, pipeline_args = parser.parse_known_args()
    input_data = read_json(known_args.input)
    clean_data = cleaning_data(input_data)
    data_with_weight = add_weight(clean_data)
    print(data_with_weight)