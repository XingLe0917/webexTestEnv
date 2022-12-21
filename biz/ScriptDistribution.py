import os
from .ConnJenkins import ConnJenkins


def address_server_name(server_name):
    server_name_rst = filter(str.isalnum, server_name)
    return ("".join(list(server_name_rst))) + ".webex.com"


def address_server_name_list(server_name_list):
    # List type:[('aaaaa',), ('bbbbb',),...]
    server_name_list = list(map(address_server_name, server_name_list))
    return server_name_list




def split_script_dir_and_filename(input_script):
    script_name_list = input_script.split("/")
    script_name = input_script
    script_dir = ""
    if len(script_name_list) > 2:
        script_name = script_name_list[-1]
        script_dir = "/".join(script_name_list[0:-1]) + "/"
    return script_name, script_dir


def get_server_name_list_from_config_file(filename):
    with open(filename, "r") as f:
        line = f.readlines()
        line = [x.strip() for x in line if x.strip() != '']
    return line


def compare_distributing_script_preview(source_file, target_dir):
    for path in [source_file, target_dir]:
        if not os.path.exists(path):
            return {
                "status": "fail",
                "compareReport": "Cannot Find " + path,
            }
    old_file_content = []
    try:
        f = open(source_file, 'r')
        file_content_list = f.read().split("\n")
    except Exception as e:
        return {
            "status": "fail",
            "compareReport": str(e)
        }
    target_old_file = target_dir + "/" + source_file.split("/")[-1]
    if os.path.exists(target_old_file):
        try:
            f = open(target_old_file, 'r')
            old_file_content = f.read().split("\n")
        except Exception as e:
            return {
                "status": "fail",
                "compareReport": str(e)
            }
    compared_list = compare_2_rst(old_file_content, file_content_list)
    compare_report_html = "<br />".join(compared_list)
    return {
        "status": "success",
        "compareReport": compare_report_html
    }


def compare_2_rst(rst1, rst2):
    new_compared_file_list = []
    old_compared_file_list = []
    num2 = len(rst2)
    rst2_listed_index = 0
    for i, element in enumerate(rst1):
        rst2_start_index = rst2_listed_index
        while rst2_start_index < num2:
            if rst1[i] == rst2[rst2_start_index]:
                if rst2_listed_index < rst2_start_index:
                    for index in range(rst2_listed_index, rst2_start_index):
                        new_compared_file_list.append("<span style='color: green;margin: 1rem;font-size: large;'>" + rst2[index] + "</span>")
                        old_compared_file_list.append("<span style='color: red;margin: 1rem;font-size: large;'>" + "/" * len(rst2[index]) + "</span>")
                        rst2_listed_index += 1
                if compare_row(rst1[i], rst2[rst2_start_index]):
                    new_compared_file_list.append("<span style='margin: 1rem;font-size: large;'>" + rst1[i] + "</span>")
                    old_compared_file_list.append(
                        "<span style='margin: 1rem;font-size: large;'>" +
                        rst1[i] + "</span>")
                    rst2_listed_index += 1
                else:
                    new_compared_file_list.append("<span style='color: red;margin: 1rem;font-size: large;'>" + "/" * len(rst1[i]) + "</span>")
                    old_compared_file_list.append("<span style='color: green;margin: 1rem;font-size: large;'>" + rst1[i] + "</span>")
                    new_compared_file_list.append("<span style='color: green;margin: 1rem;font-size: large;'>" + rst2[rst2_listed_index] + "</span>")
                    old_compared_file_list.append("<span style='color: red;margin: 1rem;font-size: large;'>" + "/" * len(rst2[rst2_listed_index]) + "</span>")
                    rst2_listed_index += 1
                break
            elif rst2_start_index == num2 - 1:
                new_compared_file_list.append("<span style='color: red;margin: 1rem;font-size: large;'>" + "/" * len(element) + "</span>")
                old_compared_file_list.append("<span style='color: green;margin: 1rem;font-size: large;'>" + element + "</span>")
                rst2_start_index += 1
            else:
                rst2_start_index += 1
    return new_compared_file_list, old_compared_file_list


def compare_row(row1, row2):
    return True if row1 == row2 else False


def distribute_script_by_jenkins(server_list_file, source_file, target_dir):
    if not server_list_file:
        return {
            "status": "fail"
        }
    target_server = ""
    target_server_list = server_list_file.split("\r\n")
    for line in target_server_list:
        target_server += line + ".webex.com "
    ConnJenkins().build_jenkins_job("distributing_script_to_server", target_server, source_file, target_dir)
    return {
        "status": "success"
    }


def get_distribute_log_from_jenkins():
    job_name = "distributing_script_to_server"
    last_build_number = ConnJenkins().get_job_number(job_name)
    if not last_build_number:
        return {
            "status": "fail",
            "logDictArray": "the jenkins job does not exist"
        }
    log_dict_list = []
    offset_num = 1
    if last_build_number > 5:
        offset_num = last_build_number - 4
    for i in range(offset_num, last_build_number + 1):
        build_info = ConnJenkins().get_build_info(job_name, i)
        if not build_info:
            return {
                "status": "fail",
                "logDictArray": "the jenkins job does not exist"
            }
        log_dict_list.append({
            "buildName": build_info["fullDisplayName"],
            "buildUrl": build_info["url"]
        })
    return {
        "status": "success",
        "logDictArray": log_dict_list
    }


if __name__ == '__main__':
    src_file = "C://Users//yejfeng//Desktop//workspace//CCP_integrated//CCP//WebexTestEnv//ccpserver.py"
    tgt_file = "C://Users//yejfeng//Desktop//workspace//CCP_integrated//CCP//WebexTestEnv//testserver.py"
    src_file_list = open(src_file, "r").read().split("\n")
    tgt_file_list = open(tgt_file, "r").read().split("\n")
    # for i in range(0, len(src_file_list)):
    #     print(src_file_list[i])
    # print("=============================")
    # for i in range(0, len(tgt_file_list)):
    #     print(tgt_file_list[i])
    new_compared_file_list, old_compared_file_list = compare_2_rst(src_file_list, tgt_file_list)
    for i in range(0, len(new_compared_file_list)):
        print(str(i) + " " + new_compared_file_list[i] + "</br>")
    print("========================================")
    for i in range(0, len(old_compared_file_list)):
        print(str(i) + " " + old_compared_file_list[i] + "</br>")
