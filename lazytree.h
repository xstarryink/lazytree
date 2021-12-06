/*
wangxin 2020-09-30
懒树 lazytree
*/
#ifndef __lazytree_h__
#define __lazytree_h__

class lazytree
{

	struct tree_node
	{
		unsigned long place;
		tree_node *left;
		tree_node *right;

		tree_node()
			:place(0)
			, left(nullptr)
			, right(nullptr)
		{

		}
	};

public:

    lazytree()
		:m_root(nullptr)
		, m_size(0)
	{
	}

    ~lazytree()
	{
		clear();
	}

	void insert(unsigned long key)
	{
		//因为构建的小顶堆是以1为根节点的（这样每一层第一个数都是2的幂级数，方便计算和阅读）
		//所以为了存储数字0，需要将所有的key加1，转化为这个key所对应的位置place。
		if (m_root)
		{
			//若根节点已经存在，则先尝试占据（occupy）根节点。
			//是否需要将根节点上的值驱赶走(drive)，取决于place是否为1
			//(key == 0) <==> (place == 1)，用来检验是不是应该占据根节点（即1结点）
			if (key == 0)
			{
				//需要将根节点上的值驱赶走，并占据根节点
				if (occupy(0, m_root, m_root->place, true))
				{
					//key == 0,place则等1
					m_root->place = 1;
					++m_size;
				}
			}
			else
			{
				//不需要进行驱赶
				if (occupy(0, m_root, key + 1, false))
				{
					++m_size;
				}
			}
		}
		else
		{
			//若根节点不存在，直接将该值赋值给根节点
			m_root = new tree_node;
			m_root->place = key + 1;
			++m_size;
		}
	}

	void erase(unsigned long key)
	{
		//如果根节点为空，那必然不存在key
		if (!m_root) return;
		//因为移除时，可能需要解除和父节点的关系（有两种移除策略），所以保存父节点
		tree_node *current = m_root,
			*parent = nullptr;

		//因为构建的小顶堆是以1为根节点的（这样每一层第一个数都是2的幂级数，方便计算和阅读）
		//所以为了存储数字0，需要将所有的key加1，转化为这个key所对应的位置place。
		unsigned long place = key + 1;
		//计算对数，获得place的所在的高度，得到最多需要走几步，即循环次数
		unsigned long height = log2_32bit(place);
		for (int i = height - 1; i > -1; --i)
		{
			if (current->place == place)
			{
				//如果找到了目标，就跳出循环
				break;
			}
			parent = current;
			//判断下一步是往左子节点走还是往右子节点走
			current = ((place >> i) & 1) ? current->right : current->left;
			//如果走到了空结点，即说明树中无匹配对象
			if (!current) return;
		}
		//需要判断是跳出的循环，还是循环结束
		if (current->place == place)
		{
			//优先向右继续往下层遍历（也可以优先向左），将子孙中的叶子结点换上来
			tree_node *_child = current->right ? current->right : current->left;
			if (_child)
			{
				//如果存在子节点，则继续向下知道叶子节点
				tree_node *_grandparent = nullptr,
					*_parent = current;
				while (_child)
				{
					_grandparent = _parent;
					_parent = _child;
					_child = _parent->right ? _parent->right : _parent->left;
				}
				//将叶子结点的值赋值给当前结点，并删除该叶子结点
				current->place = _parent->place;
				if (_grandparent->left == _parent)
				{
					_grandparent->left = nullptr;
				}
				else
				{
					_grandparent->right = nullptr;
				}
				delete _parent;
				--m_size;
			}
			else
			{
				//本身就是一个叶子结点，则直接删除
				if (parent)
				{
					if (parent->left == current)
					{
						parent->left = nullptr;
					}
					else
					{
						parent->right = nullptr;
					}
				}
				else
				{
					//无父节点，则说明是根节点
					m_root = nullptr;
				}
				delete current;
				--m_size;
			}
		}
	}

	bool find(unsigned long key)
	{
		//如果根节点为空，那必然不存在key
		if (!m_root) return false;
		tree_node *current = m_root;

		//因为构建的小顶堆是以1为根节点的（这样每一层第一个数都是2的幂级数，方便计算和阅读）
		//所以为了存储数字0，需要将所有的key加1，转化为这个key所对应的位置place。
		unsigned long place = key + 1;
		//计算对数，获得place的所在的高度，得到最多需要走几步，即循环次数
		unsigned long height = log2_32bit(place);
		for (int i = height - 1; i > -1; --i)
		{
			if (current->place == place)
			{
				//找到了目标
				return true;
			}
			//判断下一步是往左子节点走还是往右子节点走
			current = ((place >> i) & 1) ? current->right : current->left;
			//如果走到了空结点，即说明树中无匹配对象
			if (!current) return false;
		}
		//判断最后一个结点是否匹配
		return current->place == place;
	}

	unsigned long size()
	{
		return m_size;
	}

	void clear()
	{
		remove(m_root, nullptr);
		m_root = nullptr;
		m_size = 0;
	}

private:

	//参数
	//current_height：当前所在的高度（或者说层级）
	//current：当前进行操作的结点
	//place：需要继续“回家”的值（可能是插入的值，也可能是其他被驱赶上来的值，也可能是当前结点被驱赶的值）
	//drive：是否发生了驱赶
	inline bool occupy(unsigned long current_height, tree_node *current, unsigned long place, bool drive)
	{
		//place所对应的二进制不为0的最高位，代表place实际位置所在的高度。
		//从这一位开始往后的每一位（不包括这一位），代表要到达place实际所在位置，从根节点开始所要走的每一步的方向。
		//0代表向左子节点，1代表向右子节点
		//例如13（1101），不为0的最高位在第4位，代表实际位置所在的高度为4，要到达这里得从根节点开始101（右左右）

		//计算对数，获得place的所在的高度
		//计算current高度和place所在高度的差值，得到最多需要走几步，即循环次数
		unsigned long height = log2_32bit(place),
			diff_height = log2_32bit(place) - current_height;
		//i为右移的位数，从当前高度还没走的下一步开始走
		for (int i = diff_height - 1; i > -1; --i)
		{
			if (!drive && current->place == place)
			{
				//如果没有发生驱赶现象，那么出现相等的情况，就意味着已经存在，不需要再插入
				return false;
			}
			//如果发生驱赶或者place还不等于当前的位置，那么就需要继续向下走。
			//因为继续往下，不能再出现和place一样的值（或者说如果有一样的值，会在插入时，碰到place的时候停下）
			//判断下一步是往左子节点走还是往右子节点走
			tree_node *&tmp = ((place >> i) & 1) ? current->right : current->left;
			if (tmp)
			{
				//如果下一步的结点存在，那么将该节点设为当前结点，继续进行循环
				current = tmp;
			}
			else
			{
				//如果下一步的结点不存在，那么创建结点，并进行赋值，插入结束
				tmp = new tree_node;
				tmp->place = place;
				return true;
			}
		}
		//没有创建新的结点，插入也没有结束，也就意味着place来到了真正属于他的位置。
		if (current->place != place)
		{
			//如果当前结点上的值不为place，就需要进行驱赶，将这个位置上的值驱赶走。
			//当前所在高度，即为place的实际位置高度
			if (current->place)
			{
				occupy(height, current, current->place, true);
			}
			current->place = place;
			return true;
		}
		return false;
	}

	inline void remove(tree_node *node,tree_node *parent)
	{
		if (node)
		{
			if (parent)
			{
				//如果有父节点，需要需要断绝父子关系，将对应子节点置空
				if (parent->left == node)
				{
					parent->left = nullptr;
				}
				else
				{
					parent->right = nullptr;
				}
			}
			//如果还有子节点，则需要删除
			//因为该节点本身已经失去了意义，所以其子节点的父节点可以传空，不需要断绝父子关系
			if (node->left)
			{
				remove(node->left, nullptr);
			}
			if (node->right)
			{
				remove(node->right, nullptr);
			}
			delete node;
		}
	}

	inline unsigned long higher_power_2(unsigned long data)
	{
		--data;
		data |= data >> 1;
		data |= data >> 2;
		data |= data >> 4;
		data |= data >> 8;
		data |= data >> 16;
		++data;
		return data;
	}

	inline unsigned long log2_32bit_for_2power(unsigned long v)
	{
		switch (v)
		{
		case 1:
			return 0;
		case 2:
			return 1;
		case 4:
			return 2;
		case 8:
			return 3;
		case 16:
			return 4;
		case 32:
			return 5;
		case 64:
			return 6;
		case 128:
			return 7;
		case 256:
			return 8;
		case 512:
			return 9;
		case 1024:
			return 10;
		case 2048:
			return 11;
		case 4096:
			return 12;
		case 8192:
			return 13;
		case 16384:
			return 14;
		case 32768:
			return 15;
		case 65536:
			return 16;
		case 131072:
			return 17;
		case 262144:
			return 18;
		case 524288:
			return 19;
		case 1048576:
			return 20;
		case 2097152:
			return 21;
		case 4194304:
			return 22;
		case 8388608:
			return 23;
		case 16777216:
			return 24;
		case 33554432:
			return 25;
		case 67108864:
			return 26;
		case 134217728:
			return 27;
		case 268435456:
			return 28;
		case 536870912:
			return 29;
		case 1073741824:
			return 30;
		case 2147483648:
			return 31;
		default:
			throw;
			break;
		}
	}

	inline unsigned long log2_32bit(unsigned long v)
	{
		static const char log_table_256[256] =
		{
#define LT(n) n, n, n, n, n, n, n, n, n, n, n, n, n, n, n, n
			- 1, 0, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3,
			LT(4), LT(5), LT(5), LT(6), LT(6), LT(6), LT(6),
			LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7), LT(7)
		};

		unsigned r;// r will be lg(v)
		unsigned long t, tt;// temporaries

		if (tt = v >> 16)
		{
			r = (t = tt >> 8) ? 24 + log_table_256[t] : 16 + log_table_256[tt];
		}
		else
		{
			r = (t = v >> 8) ? 8 + log_table_256[t] : log_table_256[v];
		}
		return r;
	}

private:

	tree_node *m_root;

	unsigned long m_size;

};

#endif //__lazytree_h__
