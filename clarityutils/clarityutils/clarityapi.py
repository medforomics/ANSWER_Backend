import sys
from xml.dom.minidom import parseString
import urllib

MONTHS = ['ZERO', 'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October',
          'November', 'December']

def setup_api(hostname, example_uri, version):
    """
    sets up the API, returns a clarity api instance
    :param hostname:
    :param example_uri:
    :param version:
    :return: clarityapi instance
    """


def get_xml_attribute(dom, tag, attribute, default):
    try:
        return dom.getElementsByTagName(tag)[0].getAttribute(attribute)
    except IndexError:
        return default


def get_xml_element_data(dom, tag, default):
    try:
        return dom.getElementsByTagName(tag)[0].firstChild.data
    except IndexError:
        return default


class Error(Exception):
    pass


class UdfNotFoundError(Error):
    def __init__(self, name, message):
        self.message = message
        self.name = name


class Udf:
    def __init__(self, name, udf_type, value):
        self.name = name
        self.type = udf_type
        self.value = value


def get_udfs_from_dom(dom, udfs):
    udf_nodes = dom.getElementsByTagName("udf:field")
    for udf_node in udf_nodes:
        udf_name = udf_node.getAttribute("name")
        udf_type = udf_node.getAttribute("type")
        if udf_node.firstChild is None:
            udf_value = None
        else:
            udf_value = udf_node.firstChild.data
        udfs.append(Udf(udf_name, udf_type, udf_value))
    pass


class Project:
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('prj:project')[0].getAttribute('uri')
        self.limsid = self.dom.getElementsByTagName('prj:project')[0].getAttribute('limsid')
        self.name = self.dom.getElementsByTagName("name")[0].firstChild.data
        self.udfs = []
        self.researcher_uri = self.dom.getElementsByTagName('researcher')[0].getAttribute('uri')
        self.open_date = self.dom.getElementsByTagName('open-date')[0].firstChild.data
        get_udfs_from_dom(self.dom, self.udfs)

    def get_samples(self, api):
        result = api.GET(api.base_uri + 'samples/?projectname=' + self.name)
        result_dom = parseString(result)
        nodes = result_dom.getElementsByTagName('sample')
        sample_limsids = []
        for node in nodes:
            sample_limsids.append(node.getAttribute('limsid'))
        return sample_limsids

    def get_udf(self, name):
        for each in self.udfs:
            if each.name == name:
                return each

        raise UdfNotFoundError("UDF not found", name)

    def set_udf(self, name, value, udf_type=''):
        for each in self.udfs:
            if each.name == name:
                each.name = name
                each.value = value
                return
        self.udfs.append(Udf(name, udf_type, value))
        return

    def get_xml(self):
        xml = '<prj:project xmlns:udf="http://genologics.com/ri/userdefined" xmlns:ri="http://genologics.com/ri" ' \
              'xmlns:file="http://genologics.com/ri/file" xmlns:prj="http://genologics.com/ri/project" uri="{uri}" ' \
              'limsid="{limsid}">\n '
        xml += '<name>{name}</name>\n'
        xml += '<open-date>{open_date}</open-date>\n'
        xml += '<researcher uri="{researcher_uri}"/>\n'
        for each in self.udfs:
            xml += '<udf:field type="{udf_type}" name="{udf_name}">{udf_value}</udf:field>\n'.format(udf_type=each.type,
                                                                                                     udf_name=each.name,
                                                                                                     udf_value=each.value)
        xml += '</prj:project>'
        xml = xml.format(uri=self.uri, limsid=self.limsid, name=self.name, open_date=self.open_date,
                         researcher_uri=self.researcher_uri)
        return xml


class InputOutputMap(object):
    def __init__(self, node):
        self.input_uri = node.getElementsByTagName('input')[0].getAttribute('uri')
        self.input_post_process_uri = node.getElementsByTagName('input')[0].getAttribute('post-process-uri')
        self.input_limsid = node.getElementsByTagName('input')[0].getAttribute('limsid')
        self.input_parent_process_uri = get_xml_attribute(node, 'parent-process', 'uri', None)
        self.input_parent_process_limsid = get_xml_attribute(node, 'parent-process', 'limsid', None)
        self.output_uri = get_xml_attribute(node, 'output', 'uri', None)
        self.output_generation_type = get_xml_attribute(node, 'output', 'output-generation-type', None)
        self.output_type = get_xml_attribute(node, 'output', 'output-type', None)
        self.output_limsid = get_xml_attribute(node, 'output', 'limsid', None)


class Process(object):
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('prc:process')[0].getAttribute('uri')
        self.limsid = self.dom.getElementsByTagName('prc:process')[0].getAttribute('limsid')
        self.type_uri = self.dom.getElementsByTagName('type')[0].getAttribute('uri')
        self.type_name = self.dom.getElementsByTagName('type')[0].firstChild.data
        self.date_run = get_xml_element_data(self.dom, 'date-run', None)
        self.technician_uri = self.dom.getElementsByTagName('technician')[0].getAttribute('uri')
        self.technician_first_name = self.dom.getElementsByTagName('first-name')[0].firstChild.data
        self.technician_last_name = self.dom.getElementsByTagName('last-name')[0].firstChild.data
        self.input_output_maps = [InputOutputMap(x) for x in self.dom.getElementsByTagName('input-output-map')]
        self.udfs = []
        get_udfs_from_dom(self.dom, self.udfs)
        self.process_parameter_name = get_xml_attribute(self.dom, 'process-parameter', 'name', None)


class ResearcherRole(object):
    def __init__(self, node):
        self.uri = node.getAttribute('uri')
        self.name = node.getAttribute('name')
        self.role_name = node.getAttribute('roleName')


class Researcher(object):
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('res:researcher')[0].getAttribute('uri')
        self.limsid = self.dom.getElementsByTagName('res:researcher')[0].getAttribute('limsid')
        self.first_name = self.dom.getElementsByTagName('first-name')[0].firstChild.data
        self.last_name = self.dom.getElementsByTagName('last-name')[0].firstChild.data
        self.email = self.dom.getElementsByTagName('email')[0].firstChild.data
        self.lab_uri = self.dom.getElementsByTagName('lab')[0].getAttribute('uri')
        self.username = self.dom.getElementsByTagName('username')[0].firstChild.data
        self.account_locked = self.dom.getElementsByTagName('account-locked')[0].firstChild.data
        self.roles = [ResearcherRole(x) for x in self.dom.getElementsByTagName('role')]
        self.initials = self.dom.getElementsByTagName('initials')[0].firstChild.data


class Sample(object):
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('smp:sample')[0].getAttribute('uri')
        self.limsid = self.dom.getElementsByTagName('smp:sample')[0].getAttribute('limsid')
        self.name = self.dom.getElementsByTagName("name")[0].firstChild.data
        self.date_received = self.dom.getElementsByTagName("date-received")[0].firstChild.data
        self.project_uri = get_xml_attribute(self.dom, "project", "uri", None)
        self.project_limsid = get_xml_attribute(self.dom, "project", "limsid", None)
        self.submitter_uri = self.dom.getElementsByTagName("submitter")[0].getAttribute('uri')
        self.submitter_first_name = self.dom.getElementsByTagName('first-name')[0].firstChild.data
        self.submitter_last_name = self.dom.getElementsByTagName('last-name')[0].firstChild.data
        self.artifact_uri = self.dom.getElementsByTagName('artifact')[0].getAttribute('uri')
        self.artifact_limsid = self.dom.getElementsByTagName('artifact')[0].getAttribute('limsid')
        self.udfs = []
        get_udfs_from_dom(self.dom, self.udfs)

    def get_udf(self, name):
        for each in self.udfs:
            if each.name == name:
                return each
        raise UdfNotFoundError("UDF not found", name)

    def set_udf(self, name, value, udf_type=''):
        for each in self.udfs:
            if each.name == name:
                each.name = name
                each.value = value
                return
        self.udfs.append(Udf(name, udf_type, value))
        return

    def get_xml(self):
        xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
        xml += '<smp:sample xmlns:udf="http://genologics.com/ri/userdefined" xmlns:ri="http://genologics.com/ri" ' \
              'xmlns:file="http://genologics.com/ri/file" xmlns:smp="http://genologics.com/ri/sample" uri="{uri}" ' \
              'limsid="{limsid}">\n'
        xml += '<name>{name}</name>\n<date-received>{date_received}</date-received>\n<project limsid="{' \
               'project_limsid}" uri="{project_uri}"/>\n'
        xml += '<submitter uri="{submitter_uri}">\n'
        xml += '<first-name>{submitter_first_name}</first-name>\n'
        xml += '<last-name>{submitter_last_name}</last-name>\n'
        xml += '</submitter>\n'
        xml += '<artifact limsid="{artifact_limsid}" uri="{artifact_uri}"/>\n'
        for each in self.udfs:
            xml += '<udf:field type="{udf_type}" name="{udf_name}">{udf_value}</udf:field>\n'.format(udf_type=each.type,
                                                                                                     udf_name=each.name,
                                                                                                     udf_value=each.value)
        xml += '</smp:sample>\n'
        xml = xml.format(uri=self.uri, limsid=self.limsid, name=self.name, date_received=self.date_received,
                         project_limsid=self.project_limsid, project_uri=self.project_uri,
                         submitter_uri=self.submitter_uri, artifact_limsid=self.artifact_limsid,
                         artifact_uri=self.artifact_uri, submitter_first_name=self.submitter_first_name,
                         submitter_last_name=self.submitter_last_name)
        return xml


class WorkflowStage:
    def __init__(self, node):
        self.status = node.getAttribute('status')
        self.name = node.getAttribute('name')
        self.uri = node.getAttribute('uri')


class Artifact(object):
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('art:artifact')[0].getAttribute('uri')
        self.limsid = self.dom.getElementsByTagName('art:artifact')[0].getAttribute('limsid')
        self.name = self.dom.getElementsByTagName('name')[0].firstChild.data
        self.type = self.dom.getElementsByTagName('type')[0].firstChild.data
        self.parent_process_uri = get_xml_attribute(self.dom, 'parent-process', 'uri', '')
        self.parent_process_limsid = get_xml_attribute(self.dom, 'parent-process', 'limsid', '')
        self.output_type = self.dom.getElementsByTagName('output-type')[0].firstChild.data
        self.qc_flag = self.dom.getElementsByTagName('qc-flag')[0].firstChild.data
        self.container_uri = get_xml_attribute(self.dom, 'container', 'uri', '')
        self.container_limsid = get_xml_attribute(self.dom, 'container', 'limsid', '')
        self.container_value = get_xml_element_data(self.dom, 'value', '')
        self.working_flag = get_xml_element_data(self.dom, 'working-flag', '')
        self.sample_uri = self.dom.getElementsByTagName('sample')[0].getAttribute('uri')
        self.sample_limsid = self.dom.getElementsByTagName('sample')[0].getAttribute('limsid')
        self.workflow_stages = [WorkflowStage(x) for x in self.dom.getElementsByTagName('workflow-stage')]
        self.udfs = []
        get_udfs_from_dom(self.dom, self.udfs)

    pass

    def set_udf(self, name, value, udf_type=''):
        for each in self.udfs:
            if each.name == name:
                each.name = name
                each.value = value
                return
        self.udfs.append(Udf(name, udf_type, value))
        return

    def get_xml(self):
        xml = '<art:artifact xmlns:udf="http://genologics.com/ri/userdefined" xmlns:file="http://genologics.com/ri/file"' \
              ' xmlns:art="http://genologics.com/ri/artifact" uri="{uri}" limsid="{limsid}">\n'.format(uri=self.uri,
                                                                                                       limsid=self.limsid)
        xml += '\t<name>{name}</name>\n'.format(name=self.name)
        xml += '\t<type>{type}</type>\n'.format(type=self.type)
        xml += '\t<output-type>{output_type}</output-type>\n'.format(output_type=self.output_type)
        if self.parent_process_uri != '':
            xml += '\t<parent-process uri="{parent_process_uri}" limsid="{parent_process_limsid}"/>\n'.format(
                parent_process_uri=self.parent_process_uri, parent_process_limsid=self.parent_process_limsid)
        xml += '\t<qc-flag>{qc_flag}</qc-flag>\n'.format(qc_flag=self.qc_flag)
        if self.container_uri != '':
            xml += '\t<location>\n'
            xml += '\t\t<container uri="{container_uri}" limsid="{container_limsid}"/>\n'.format(
                container_uri=self.container_uri, container_limsid=self.container_limsid)
            xml += '\t\t<value>{container_value}</value>\n'.format(container_value=self.container_value)
            xml += '\t</location>\n'
        if self.working_flag != '':
            xml += '\t<working-flag>{working_flag}</working-flag>\n'.format(working_flag=self.working_flag)
        xml += '\t<sample uri="{uri}" limsid="{limsid}"/>\n'.format(uri=self.sample_uri, limsid=self.sample_limsid)
        for each in self.udfs:
            xml += '\t<udf:field type="{udf_type}" name="{udf_name}">{udf_value}</udf:field>\n'.format(
                udf_type=each.type,
                udf_name=each.name,
                udf_value=each.value)
        if self.workflow_stages:
            xml += '\t<workflow-stages>\n'
            for workflow_stage in self.workflow_stages:
                xml += '\t\t<workflow-stage status="{status}" name="{name}" uri="{uri}"/>\n'.format(
                    status=workflow_stage.status, name=workflow_stage.name, uri=workflow_stage.uri)
            xml += '\t</workflow-stages>\n'
        else:
            xml += '\t<workflow-stages/>\n'
        xml += '</art:artifact>\n'
        return xml

    def get_udf(self, name):
        for each in self.udfs:
            if each.name == name:
                return each
        raise UdfNotFoundError("UDF not found", name)


class ContainerType(object):
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('ctp:container-type')[0].getAttribute('uri')
        self.name = self.dom.getElementsByTagName('ctp:container-type')[0].getAttribute('name')
        self.is_tube = self.dom.getElementsByTagName('is-tube')[0].firstChild.data
        # TODO: Implement X and Y dimensions


class Placement(object):
    def __init__(self, node):
        self.uri = node.getAttribute('uri')
        self.limsid = node.getAttribute('limsid')
        self.value = node.getElementsByTagName('value')[0].firstChild.data


class Container(object):
    def __init__(self, xml):
        self.dom = parseString(xml)
        self.uri = self.dom.getElementsByTagName('con:container')[0].getAttribute('uri')
        self.limsid = self.dom.getElementsByTagName('con:container')[0].getAttribute('limsid')
        self.name = self.dom.getElementsByTagName('name')[0].firstChild.data
        self.type_uri = self.dom.getElementsByTagName('type')[0].getAttribute('uri')
        self.type_name = self.dom.getElementsByTagName('type')[0].getAttribute('name')
        self.occupied_wells = int(self.dom.getElementsByTagName('occupied-wells')[0].firstChild.data)
        placement = self.dom.getElementsByTagName('placement')
        if len(placement) is not 0:
            self.placement = Placement(self.dom.getElementsByTagName('placement')[0])
        else:
            self.placement = None
        self.state = self.dom.getElementsByTagName('state')[0].firstChild.data

    def get_xml(self):
        xml = '<con:container xmlns:udf="http://genologics.com/ri/userdefined" ' \
              'xmlns:con="http://genologics.com/ri/container" uri="{uri}" limsid="{limsid}">\n '
        xml += '<name>{name}</name>\n'
        xml += '<type uri="{type_uri}" name="{type_name}"/>\n'
        xml += '<occupied-wells>{occupied_wells}</occupied-wells>\n'
        if self.placement is not None:
            xml += '<placement uri="{placement_uri}" limsid="{placement_limsid}">\n'.format(
                placement_uri=self.placement.uri, placement_limsid=self.placement.limsid)
            xml += '<value>{placement_value}</value>\n'.format(placement_value=self.placement.value)
            xml += '</placement>\n'
        xml += '<state>{state}</state>\n'
        xml += '</con:container>\n'
        xml = xml.format(uri=self.uri, limsid=self.limsid, name=self.name, type_uri=self.type_uri,
                         type_name=self.type_name, occupied_wells=self.occupied_wells, placement_uri=self.placement.uri,
                         placement_limsid=self.placement.limsid, placement_value=self.placement.value, state=self.state)
        return xml


def get_researcher_uri(first_name, last_name, api):
    query = api.base_uri + 'researchers?firstname=' + first_name + '&lastname=' + last_name
    query_xml = api.GET(query)
    query_dom = parseString(query_xml)
    researcher_uri = query_dom.getElementsByTagName('researcher')[0].attributes['uri'].value
    return researcher_uri


def get_researcher_by_name(first_name, last_name, api):
    query_xml = api.GET(api.base_uri + 'researchers?firstname={first_name}&lastname={last_name}'.format(
        first_name=first_name, last_name=last_name))
    query_dom = parseString(query_xml)
    researcher_uri = query_dom.getElementsByTagName('researcher')[0].getAttribute('uri')
    researcher_xml = api.GET(api.base_uri + 'researchers/' + researcher_uri.split('/')[-1])
    return Researcher(researcher_xml)


def create_project_with_client(project_name, udfs, project_date, researcher_uri, api):
    project_limsid = ""

    query_uri = api.base_uri + "projects?name=" + urllib.quote(project_name)
    query_xml = api.GET(query_uri)
    query_dom = parseString(query_xml)

    nodes = query_dom.getElementsByTagName("project")
    node_count = len(nodes)

    if node_count == 0:
        # create a new project

        project_xml = '<?xml version="1.0" encoding="utf-8"?>'
        project_xml += '<prj:project xmlns:udf="http://genologics.com/ri/userdefined" ' \
                       'xmlns:ri="http://genologics.com/ri" xmlns:file="http://genologics.com/ri/file" ' \
                       'xmlns:prj="http://genologics.com/ri/project"> '
        project_xml += '<name>' + project_name + '</name>'
        project_xml += '<open-date>' + project_date + '</open-date>'
        project_xml += '<researcher uri="' + researcher_uri + '"/>'

        for each in udfs:
            project_xml += '<udf:field type="{type}" name="{name}">{value}</udf:field>'.format(
                type=each.type,
                name=each.name,
                value=each.value
            )
        project_xml += '</prj:project>'
        project_xml = project_xml.encode("utf-8")

        # create this in LIMS
        rXML = api.POST(project_xml, api.base_uri + "projects")
        try:
            rDOM = parseString(rXML)
            nodes = rDOM.getElementsByTagName("prj:project")
            if len(nodes) > 0:
                project_uri = nodes[0].getAttribute("uri")
                project_limsid = nodes[0].getAttribute("limsid")
                log("Created Project: " + project_limsid + " with Name:" + project_name)
            else:
                log("ERROR: Creating Project")
                log(rXML)
        except:
            log("ERROR: Creating Project")
            log(rXML)

    elif node_count == 1:
        # we have a project, return the limsid
        log("Project with name: " + project_name + " already in system")
        project_uri = nodes[0].getAttribute("uri")
        project_limsid = nodes[0].getAttribute("limsid")
    else:
        # this is bad: we have multiple projects with the same name!!!!!
        log("Multiple Project already exist for: " + project_name)
        sys.exit(3)
    return project_limsid


def create_project(pName, udfs, pDate, api):
    pLIMSID = ""
    project_uri = ""

    qURI = api.base_uri + "projects?name=" + urllib.quote(pName)
    qXML = api.GET(qURI)
    qDOM = parseString(qXML)

    ## did we get any project nodes?
    nodes = qDOM.getElementsByTagName("project")
    nodeCount = len(nodes)

    if nodeCount == 0:
        ## create a new project

        pXML = '<?xml version="1.0" encoding="utf-8"?>'
        pXML += '<prj:project xmlns:udf="http://genologics.com/ri/userdefined" xmlns:ri="http://genologics.com/ri" xmlns:file="http://genologics.com/ri/file" xmlns:prj="http://genologics.com/ri/project">'
        pXML += '<name>' + pName + '</name>'
        pXML += '<open-date>' + pDate + '</open-date>'
        pXML += '<researcher uri="' + api.base_uri + 'researchers/1"/>'
        ## add the udfs
        # Assumes all types are strings

        for udf in udfs:
            pXML += '<udf:field type="{type}" name="{name}">{value}</udf:field>'.format(
                type=udf.type,
                name=udf.name,
                value=udf.value
            )
        pXML += '</prj:project>'
        pXML = pXML.encode("utf-8")
        print(pXML)
        ## create this in LIMS
        rXML = api.POST(pXML, api.base_uri + "projects")
        try:
            rDOM = parseString(rXML)
            nodes = rDOM.getElementsByTagName("prj:project")
            if len(nodes) > 0:
                project_uri = nodes[0].getAttribute("uri")
                pLIMSID = nodes[0].getAttribute("limsid")
                log("Created Project: " + pLIMSID + " with Name:" + pName)
            else:
                log("ERROR: Creating Project")
                log(rXML)
        except:
            log("ERROR: Creating Project")
            log(rXML)

    elif nodeCount == 1:
        ## we have a project, return the limsid
        log("Project with name: " + pName + " already in system")
        project_uri = nodes[0].getAttribute("uri")
    else:
        ## this is bad: we have multiple projects with the same name!!!!!
        log("Multiple Project already exist for: " + pName)
        sys.exit(3)
    return project_uri


def get_container_type_uri(container_type_name, api):
    query_url = api.base_uri + 'containertypes?name=' + urllib.quote(container_type_name)
    query_xml = api.GET(query_url)
    query_dom = parseString(query_xml)
    nodes = query_dom.getElementsByTagName("container-type")
    if len(nodes) == 0:
        # TODO: Add Logging
        print("Container type " + container_type_name + " not found.")
        exit()
    container_type_uri = nodes[0].getAttribute("uri")
    return container_type_uri


def create_container_xml(container_type_uri, container_name):
    xml = '<?xml version="1.0" encoding="UTF-8"?>'
    xml += '<con:container xmlns:con="http://genologics.com/ri/container">'
    if len(container_name) > 0:
        xml += ('<name>' + container_name + '</name>')
    else:
        xml += '<name></name>'
    xml += '<type uri="' + container_type_uri + '"/>'
    xml += '</con:container>'
    return xml


def create_container(cType, cName, api):
    limsid = ""

    qURI = api.base_uri + "containertypes?name=" + urllib.quote(cType)
    qXML = api.GET(qURI)
    qDOM = parseString(qXML)

    nodes = qDOM.getElementsByTagName("container-type")
    if len(nodes) == 1:
        ctURI = nodes[0].getAttribute("uri")

        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<con:contaner xmlns:con="http://genologics.com/ri/container">'
        if len(cName) > 0:
            xml += ('<name>' + cName + '</name>')
        else:
            xml += '<name></name>'
        xml += '<type uri="' + ctURI + '"/>'
        xml += '</con:container>'

        rXML = api.POST(xml, api.base_uri + "containers")

        rDOM = parseString(rXML)
        nodes = rDOM.getElementsByTagName("con:container")
        if len(nodes) > 0:
            tmp = nodes[0].getAttribute("limsid")
            limsid = tmp

    return limsid


def log(msg):
    # global LOG
    # LOG.append( msg )
    # logging.info(msg)
    print(msg)


def create_sample_xml(sample_name, udfs, project_uri, container_uri, date_received, well_position):
    sample_xml = '<smp:samplecreation xmlns:smp="http://genologics.com/ri/sample" ' \
                 + 'xmlns:udf="http://genologics.com/ri/userdefined">'
    sample_xml += '<name>' + sample_name + '</name>'
    sample_xml += '<date-received>' + date_received + '</date-received>'
    sample_xml += '<project uri="' + project_uri + '"></project>'
    sample_xml += '<location>'
    sample_xml += '<container uri="' + container_uri + '"></container>'
    sample_xml += '<value>' + well_position + '</value>'
    sample_xml += '</location>'
    # Add the udfs
    for each in udfs:
        sample_xml += '<udf:field type="{type}" name="{name}">{value}</udf:field>'.format(
            type=each.type,
            name=each.name,
            value=each.value
        )
    sample_xml += '</smp:samplecreation>'

    return sample_xml


def get_uri_from_container(container_xml):
    container_dom = parseString(container_xml)
    nodes = container_dom.getElementsByTagName("con:container")
    container_uri = nodes[0].getAttribute("uri")
    return container_uri


def get_project_uri(project_name, api):
    query_url = api.base_uri + "projects?name=" + urllib.quote(project_name)
    query_xml = api.GET(query_url)
    query_dom = parseString(query_xml)
    nodes = query_dom.getElementsByTagName("project")
    if len(nodes) == 0:
        print("Project not found")
        return None
        # TODO: Add Logging
    project_uri = nodes[0].getAttribute['uri']
    return project_uri


def get_stage_uri(workflow_name, stage_name_to_find, api):
    # type: (str, str, glsapiutil.glsapiutil2) -> str
    stage_uri = ""

    workflows_uri = api.base_uri + "configuration/workflows"
    workflows_xml = api.GET(workflows_uri)
    workflows_dom = parseString(workflows_xml)

    workflows = workflows_dom.getElementsByTagName("workflow")
    for workflow in workflows:
        name = workflow.getAttribute("name")
        if name == workflow_name:
            workflow_uri = workflow.getAttribute("uri")
            workflow_xml = api.GET(workflow_uri)
            workflow_dom = parseString(workflow_xml)
            stages = workflow_dom.getElementsByTagName("stage")
            for stage in stages:
                stage_name = stage.getAttribute("name")
                if stage_name == stage_name_to_find:
                    stage_uri = stage.getAttribute("uri")
                    break
            break
    return stage_uri


def route_sample(sample, workflow, api):
    # type: (Sample, dict, glsapiutil.glsapiutil2 ) -> str
    stage_uri = get_stage_uri(workflow['WF'], workflow['Stage'], api)

    routing_xml = "<rt:routing xmlns:rt=\"http://genologics.com/ri/routing\">"
    routing_xml = routing_xml + '<assign stage-uri="' + stage_uri + '">'
    routing_xml = routing_xml + '<artifact uri="' + sample.artifact_uri + '"/>'
    routing_xml += '</assign>'
    routing_xml += '</rt:routing>'
    routing_xml = routing_xml.encode("utf-8")
    response = api.POST(routing_xml, api.base_uri + "route/artifacts/")
    return response


def create_project_xml(project_name, udfs, project_date, researcher_uri):
    # type: (str, list, str, str) -> str
    project_xml = '<?xml version="1.0" encoding="utf-8"?>'
    project_xml += '<prj:project xmlns:udf="http://genologics.com/ri/userdefined" ' \
                   'xmlns:ri="http://genologics.com/ri" xmlns:file="http://genologics.com/ri/file" ' \
                   'xmlns:prj="http://genologics.com/ri/project"> '
    project_xml += '<name>' + project_name + '</name>'
    project_xml += '<open-date>' + project_date + '</open-date>'
    project_xml += '<researcher uri="' + researcher_uri + '"/>'

    for each in udfs:
        project_xml += '<udf:field type="{type}" name="{name}">{value}</udf:field>'.format(
            type=each.type,
            name=each.name,
            value=each.value
        )
    project_xml += '</prj:project>'
    project_xml = project_xml.encode("utf-8")

    return project_xml


def get_project_by_name(project_name, api):
    """Find project by name, return project object"""
    query_url = api.base_uri + "projects?name=" + urllib.quote(project_name)
    query_xml = api.GET(query_url)
    query_dom = parseString(query_xml)
    nodes = query_dom.getElementsByTagName("project")
    if len(nodes) == 0:
        print("Project not found")
        return None
        # TODO: Add Logging
    project_limsid = nodes[0].getAttribute('limsid')
    project = Project(api.GET(api.base_uri + 'projects/' + project_limsid))
    return project


def get_container_type_by_name(name, api):
    query_url = api.base_uri + 'containertypes?name={name}'.format(name=name)
    query_xml = api.GET(query_url)
    query_dom = parseString(query_xml)
    container_type_uri = query_dom.getElementsByTagName('container-type')[0].getAttribute('uri')
    container_type_limsid = container_type_uri.strip().split('/')[-1]
    container_type_xml = api.GET(api.base_uri + 'containertypes/' + container_type_limsid)
    return ContainerType(container_type_xml)
