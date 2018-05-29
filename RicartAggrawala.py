from threading import Thread
from time import sleep
import xmlrpclib
import Queue
import random

has_String = False
myNodes = []
logical_time
words = ['pumpkin', 'potato', 'rice', 'coconut', 'beet', 'corn', 'spinach', 'hazelnut']
request_queue = Queue.Queue()
response_queue = Queue.Queue()
done_nodes = []
appended = []
awaiting_final_string = False
next_request_time = 0
master_node = None
node_list = None
ip = None
port = None
id = None
word_string = ''

def find_node_by_id(value):
    nodes = [n for n in node_list if n[2] == value]
    return nodes[0] if len(nodes) == 1 else None

def timeAdvance(time):
    thread = Thread(target=time_advance_grant, args=(time, master_node))
    thread.start()

def is_master_node():
    return master_node[0] == ip and master_node[1] == port and master_node[2] == id

def get_proxy_server(node):
    return xmlrpclib.ServerProxy("http://"+str(node[0])+":"+str(node[1])+"/", allow_none=True)

def get_random_waiting_time():
    return random.randint(1,7)

def send_final_string(requester):
    node = find_node_by_id(requester)
    if node == None:
        print('Unknown ID: ' + str(requester))
        return
    done_nodes.append(node)
    print(str(len(done_nodes)) + ' of ' + str(len(node_list)))
    if (len(done_nodes) == len(node_list)):
        for n in node_list:
            get_proxy_server(n).wordStringUpdate(word_list)

def check_request_queue():
    print('Checking request queue')
    if not serviced_node == None:
        print('Error: Serviced node is not null')
        return
    node = request_queue.get()
    print('Now servicing ' + str(node))
    get_proxy_server(node).receiver.wordStringUpdate(word_string)

def check_final_string(value):
    print('-------------------')
    print('final string: ' + value)
    final_tokens = value.split()
    print('words appended by this node: ' + ' '.join([word[0] for word in appended]))
    print('-------------------')
    missing = [word for word, index in appended if not final_tokens[index] == word]
    if len(missing) == 0:
        print('All words are included in the final string')
    else:
        print('Some words are missing from the final string')
        print('Words missing from final string: ' + ' '.join(missing))

def append_random_word(value):
    tokens = value.split()
    new_word = words[random.randint(0, len(words) - 1)]
    value += ('' if len(tokens) == 0 else ' ') + new_word
    appended.append([new_word, len(tokens)])
    return value

def receive_word_string(value):
    global word_string, serviced_node, awaiting_final_string
    if is_master_node():
        print('Updated word string: ' + value)
        word_string = value
        print('Finished servicing ' + str(serviced_node))
        serviced_node = None
        check_request_queue()
    else:
        if awaiting_final_string:
            check_final_string(value)
        else:
            global next_request_time
            print('old string: ' + value)
            word_string = append_random_word(value)
            print('new string: ' + word_string)
            seconds = get_random_waiting_time()
            print('Waiting for ' + str(seconds) + ' seconds')
            next_request_time += seconds
            get_proxy_server(master_node).receiver.wordStringUpdate(word_string)
    
def receive_word_string_request(requester, time):
    global awaiting_final_string
    if awaiting_final_string:
        return
    timestamp = logical_time
    for node in myNodes:
        receive_word_string(node, timestamp)
        logical_time += 1
    node = find_node_by_id(requester)
    if node == None:
        print('Unknown ID: ' + str(requester))
        return
    if is_master_node():
        print('Receive request from ' + str(node))
        request_queue.put(node)
        check_request_queue()
        
        
def requestFinalString():
    if is_master_node():
        return

def time_advance_grant(time, master):
    global awaiting_final_string, next_request_time
    print('Time: ' + str(time))
    if (time == 20):
        awaiting_final_string = True
        print('Requesting final string from ' + str(master_node))
        get_proxy_server(master).finalWordStringRequest(id)
        return
    print 'next request time: ' + str(next_request_time)
    if (next_request_time <= time):
        get_proxy_server(master).receiver.wordStringRequest(id, time)
        
def timer():
    for time in range(1,21):
        sleep(20)
        awaiting_final_string = True
        requestFinalString()
            
def sleeper():
    time.sleep(get_random_waiting_time())
    if awaiting_final_string:
        return
    for time in range(1,21):
        sleep(1)
        print('Advance time to: ' + str(time))
        for node in node_list:
            print('sending to ' + str(node))
            proxyServer = get_proxy_server(node)
            proxyServer.receiver.timeAdvance(time)

def receiveWordStringOK(senderID, timestamp):
    global response_queue, id, logical_time
    logical_time += 1
    node = find_node_by_id(senderID)
    if node == None:
        print('Unknown ID: ' + str(requester))
        return
    if node in response_queue:
        print('Duplicate node: ' + str(requester))
        return
    print "receive OK t: "+timestamp+" id: "+id
    response_queue.put(node)
    if len(response_queue) == len(nodes):
        has_String = True
        print "Entering critical section"
        receive_word_string()
        
def hasPriority(request):
    return ((logical_time < request.timestamp) or (logical_time == request.timestamp and id < request.id))

def start(node, nodes):
    global logical_time, has_String
    myNodes = nodes
    thread = Thread(target=sleeper)
    thread.start()
    awaiting_final_string = False
    request_queue = []
    done_nodes = []
    next_request_time = -1
    has_String = is_master_node()
    if not is_master_node():
        logical_time = get_random_waiting_time()
        thread = Thread(target=sleeper, args=logical_time)
        thread.start()
        threadTimer = Thread(target=timer)
        threadTimer.start()
    