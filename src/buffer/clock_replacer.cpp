#include "buffer/clock_replacer.h"
#include <iostream>
using namespace std;

ClockReplacer::ClockReplacer(size_t num_pages) {
  hand = 0;
  num_page = num_pages;
  nodes = vector<cnode>(num_page);
}

ClockReplacer::~ClockReplacer(){};

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  cout<<"Victim Called"<<endl;
  if (Size()) {
    //寻找一个status为0的有效节点（hand指向一定有效，所以没有判断是否有效的必要）
    while (nodes.at(hand).node_status) {
      //置0
      nodes.at(hand).node_status = false;
      //hand推进
      HandInc();
    }
    *frame_id = nodes.at(hand).frame_id;
    nodes.at(hand).isNull = true;
    //删掉了hand之后，将temp重新指向下一个
    HandInc();
    Test(1);
    return true;
  }
  Test(2);
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  cout<<"Pin Called"<< " frame_id: " << frame_id <<endl;
  //如果已经有了hand
  if(Size()&&Find(frame_id)!=(size_t)-1){
    //查看删除的是否是当前hand指向的
    if(nodes.at(hand).frame_id == frame_id) {
      nodes.at(hand).isNull = true;
      //如果还有其他非空的话就更改hand
      if(Size()) HandInc();
    }
    //非当前hand指向，直接设置IsNULL
    nodes.at(Find(frame_id)).isNull = true;
  }
  Test(3);
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  cout<<"Unpin Called"<< " frame_id: " << frame_id <<endl;
  size_t temp; //临时的node,用来插入和删除
  size_t index = Find(frame_id);  //对应的下标
  if (index != (size_t)-1 && !nodes.at(index).isNull) {
    // 检查发现已经在缓冲区内，注意node必须得是非空
    //如果缓冲区内只有一个元素（就是我们unpin的元素），就直接return,因为我们找不到下一个node来swap
    if(Size()==1) return;
    nodes.at(index).node_status = true;
    nodes.at(index).isNull = false;
    //将Unpin的Node跟hand前的Node置换
    SwapNodes(index,FindUsedBeforeHand());
  } else {
    //不在缓冲区，找一个空位插入
    temp = FindNull();
    if(temp != (size_t)-1){
      nodes.at(temp).isNull = false;
      nodes.at(temp).frame_id = frame_id;
      nodes.at(temp).node_status = true;
    }
    //如果是插入的第一个元素
    if(Size() == 1) hand = temp;
  }
  Test(4);
}

size_t ClockReplacer::Size() {
  //count所有isNull==false的点的数量
  int count = 0;
  for(size_t i = 0; i < num_page; i++)
    if(!nodes.at(i).isNull) count++;
  return count;
}

//查找nodes里面的对应frame_id的下标，若无，则返回-1
size_t ClockReplacer::Find(frame_id_t frame_id) {
  if(Size() != 0){
    for(size_t i = 0; i < Size(); i++){
      if(nodes.at(i).frame_id == frame_id) return i;
    }
  }
  return -1;
}

//找到第一个Null的node
size_t ClockReplacer::FindNull() {
  for(size_t i = 0; i < num_page; i++)
    if(nodes.at(i).isNull) return i;
  return -1;
}

//交换两个node,不使用直接的cnode替换是因为可能会引起内存问题？
void ClockReplacer::SwapNodes(size_t v_1, size_t v_2) {
  cnode temp;
  temp.isNull = nodes.at(v_1).isNull;
  temp.node_status = nodes.at(v_1).node_status;
  temp.frame_id = nodes.at(v_1).frame_id;
  nodes.at(v_1).isNull = nodes.at(v_2).isNull;
  nodes.at(v_1).node_status = nodes.at(v_2).node_status;
  nodes.at(v_1).frame_id = nodes.at(v_2).frame_id;
  nodes.at(v_2).isNull = temp.isNull;
  nodes.at(v_2).node_status = temp.node_status;
  nodes.at(v_2).frame_id = temp.frame_id;
}

//起到hand++的效果
void ClockReplacer::HandInc() {
  do{
    hand = (hand + 1) % num_page;
  }while(nodes.at(hand).isNull);
}
//找到hand之前的第一个非空Node
size_t ClockReplacer::FindUsedBeforeHand() {
  size_t temp = hand==0?num_page-1:hand-1;
  while(nodes.at(temp).isNull){
    temp = temp==0?num_page-1:temp-1;
  }
  return temp;
}

//打印测试信息
void ClockReplacer::Test(size_t num) {
  cout << "Test" << num << " hand: " << hand <<endl;
  for(size_t i = 0; i < num_page; i++){
    cout << "vector" << i << " page: " << nodes.at(i).frame_id << " isNull: " << nodes.at(i).isNull<< " status: "
         << nodes.at(i).node_status<<endl;
  }
  cout << "vectorSize: " << num_page << endl;
  cout << endl;
}
