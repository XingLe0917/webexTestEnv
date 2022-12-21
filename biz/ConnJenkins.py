import jenkins


class ConnJenkins(object):
    def __init__(self):
        self.jenkins_url = "http://10.252.53.146:8080/"
        self.jenkins_usr = "admin"
        self.jenkins_api_token = "117f875f36bac73cea37db680cc9386387"
        self.server = ""

    def conn_jenkins_server(self):
        if self.server:
            pass
        self.server = jenkins.Jenkins(self.jenkins_url, username=self.jenkins_usr, password=self.jenkins_api_token)

    def is_jenkins_job_exit(self, job_name):
        self.conn_jenkins_server()
        return self.server.job_exists(job_name)

    def build_jenkins_job(self, job_name, target_server, source_file, target_dir):
        self.conn_jenkins_server()
        self.server.build_job(job_name, {'TARGET_SERVER': target_server, 'SOURCE_FILE': source_file, 'TARGET_DIR': target_dir})

    def get_job_number(self, job_name):
        if not self.is_jenkins_job_exit(job_name):
            return False
        self.conn_jenkins_server()
        return self.server.get_job_info(job_name)['lastCompletedBuild']['number']

    def get_job_config(self, job_name):
        if not self.is_jenkins_job_exit(job_name):
            return False
        job_info = self.server.get_job_config(job_name)
        return job_info

    def get_jobs(self):
        self.conn_jenkins_server()
        return self.server.get_jobs()

    def disable_job(self, job_name):
        if not self.is_jenkins_job_exit(job_name):
            return False
        return self.server.disable_job(job_name)

    def delete_job(self, job_name):
        if not self.is_jenkins_job_exit(job_name):
            return False
        return self.server.delete_job(job_name)


if __name__ == '__main__':
    jobs = ConnJenkins().get_jobs()
    dict_item = {
        "job_name": "",
        "cmd": "",
        "frequency": "",
        "slave": ""
    }
    dict_list = []
    for _job in jobs:
        job_name = _job["name"]
        print(job_name)
        _config_xml = ConnJenkins().get_job_config(job_name)
        # print(_config_xml.split("\n"))
        _config_xml_list = []
        command_list = []
        command_flag = False
        for element in _config_xml.split("\n"):
            if "<command>" in element:
                command_flag = True
                command_list.append(element)
                # continue
            if "</command>" in element:
                command_flag = False
                if "<command>" not in element:
                    command_list.append(element)
                _config_xml_list.append("\n".join(command_list))
                continue
            _config_xml_list.append(element)

        for _line in _config_xml_list:
            if "command" in _line:
                print(_line)
                command = _line.split("<command>")[-1].split("</command>")[0]
                print("command: %s" % command)
                continue
            if "daysToKeep" in _line:
                days_to_keep = _line.split("<daysToKeep>")[-1].split("</daysToKeep>")[0]
                print("days_to_keep: %s" % days_to_keep)
                continue
            if "numToKeep" in _line:
                num_to_keep = _line.split("<numToKeep>")[-1].split("</numToKeep>")[0]
                print("num_to_keep: %s" % num_to_keep)
                continue
            if "assignedNode" in _line:
                slave = _line.split("<assignedNode>")[-1].split("</assignedNode>")[0]
                print("slave: %s" % slave)
                continue
            if "<spec>" in _line:
                frequency = _line.split("<spec>")[-1].split("</spec>")[0]
                print("frequency: %s" % frequency)
                continue
        print("----------------------------")
        # break

