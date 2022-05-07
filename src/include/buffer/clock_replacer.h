//
// Bonus: Clock Replacer
//

#pragma once
#include "buffer/replacer.h"
#include <vector>
#include <unordered_map>
#include <iostream>

using namespace std;

//用来储存一个ClockNode
struct ClockNode{
 public:
  //该节点目前是否已经启用
  bool isNull = true;
  //该节点的时钟状态
  bool node_status = true;
  //该节点对应的页号
  frame_id_t frame_id = 0;
};
typedef struct ClockNode cnode;

class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the CLockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;
  size_t num_page;

 private:
  // add your own private member variables here
  //Clock的节点数组
  vector<cnode> nodes;
  //clock指向的当前的下标
  size_t hand;

  size_t Find(frame_id_t frame_id);
  size_t FindNull();
  void SwapNodes(size_t v_1, size_t v_2);
  void HandInc();
  size_t FindUsedBeforeHand();
//  void Test(size_t num);
};
