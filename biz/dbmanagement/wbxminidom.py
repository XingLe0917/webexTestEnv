from common.Config import Config
from xml.dom import minidom

class wbxminidom(object):
    def __init__(self,path=None):
    #     pass
        # self.conf=None
        # self.nodecache={}
        if path:
            self.dom=minidom.parse(path)
        else:
            self.dom = minidom.Document()
        # self.dom = None

    def getwbxdom(self,xmlpath=None):
        if not xmlpath:
            self.dom=minidom.parse(xmlpath)
        else:
            self.dom = minidom.Document()

    # input tagname, attr dict
    def createElement(self,tagname,**kargs):
        node = self.dom.createElement(tagname)
        if kargs:
            for key, val in kargs.items():
                node.setAttribute(key, val)
        return node

    def createTextElement(self,text):
        textnode = self.dom.createTextNode(text)
        return textnode

    def createCDATATextElement(self,text):
        CDATAtextnode=self.dom.createCDATASection("\n"+text+"\n")
        return CDATAtextnode

    def getNodesByTagname(self,tagname,parNode):
        nodelist = parNode.getElementsByTagName(tagname)
        return nodelist

    def getCDATANodetext(self,node,tagname):
        node_cmd = node.getElementsByTagName(tagname)
        text=""
        if len(node_cmd)>0:
            text = node_cmd[0].firstChild.wholeText.strip()
        return text

    def getNodetext(self,node,tagname):
        node_cmd = node.getElementsByTagName(tagname)
        text = ""
        if len(node_cmd) > 0:
            text = node_cmd[0].firstChild.nodeValue.strip()
        return text

    def wbxWriteXML(self,dbxmlpath):
        print(self.dom)
        with open(dbxmlpath, "w", encoding="utf-8") as f:
            self.dom.writexml(f, indent='', addindent='\t', newl='\n', encoding="utf-8")



if __name__ == '__main__':
    audvo = wbxminidom()
    # keymap = {"OS": {"id": ["c0ddd1fd5ce17367e0533908fc0a1ea6"]}}
    res={}
    args=['18ef97f3ec4f428087521c034ca60300','43a1bd78252247d48bacf535240f699c','777545b9a959486f9ba942eae2aecdc5','c432141740b44970864cc2069da7fbe6'
        ,'1146aac58aec42dbabde4e9612fc3462','2382fb44158e4dfdbb03a17eaf5b3f52','973989cbbd1b47e1a4cd94509a8e8111','264ffd0fab094fc3afb84ad1c049f789'
        ,'cfac66e3a05149089ee56dd55e30f634','09e4efd8c7cc45d4a91a8c813134fd9c','7ab36d0f9cb4404ba4f9943858d648b1','e030916769714815bfcd3adba27246c0'
        ,'587868fa227f4044963ac3699370dafb','1f85c21ea03a44d899799b492236a9a8','e45a7ba261354e13988f8e7329d5cf6c','e27b114be7fa4f54a9f172ab89ee413e'
        ,'5199a53cb80a4fcba431165f427231e1','de870fea7fcc44039a684aa1fad6bf3b','c5703cc9280e496b888af51e8f1287e7','2ac52ce860cb40c296989280d568a200'
        ,'d6d769d811f0475aa062e1f7ee85d63a','b8a57a039a6542019a3967f9555076ca','ecf247f89a7d4d64aac31fd13a24c5bf','a28cccb7e705493bb64bc6a2369df2dc'
        ,'1072c39c284c46df953650dd551a0db6','02454e429606416e972e7f8abd97224e','c67b6019eb964ab28aff1582d82def01','f6101d38afa243fab8c91e1d1e72c018'
        ,'93b4a1a5f9c541eabf5582f91cf29988','4982dbbae54d4861a80413ef637e3d62']
    for arg in args:
        text = audvo.getCommand(arg)
        print(text)