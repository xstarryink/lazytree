/*
2021-10-11 wangxin
一个基于16-字典树的懒树
为存储和获取节点位置进行了特化
*/
#ifndef __lazy_trie_16_place_h__
#define __lazy_trie_16_place_h__

#include <string>

class lazy_trie_16_place
{

public:

	//树节点
	struct node
	{
		std::string key;
		unsigned long height;
		unsigned long index;
		node *nodes[16];
	};

public:

	lazy_trie_16_place()
		:m_root()
	{
		memset(m_root, 0, sizeof(node*) << 4);
	}

	~lazy_trie_16_place()
	{
		clear();
	}

	void set(std::string key, unsigned long height, unsigned long index)
	{
		unsigned long klength = key.length(),
			klength_real = klength << 1;
		node **nodes = m_root;
		for (unsigned long i = 0; i < klength_real; ++i)
		{
			unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
			node *n = nodes[key_real];
			if (n == nullptr)
			{
				n = new node;
				n->key = key;
				n->height = height;
				n->index = index;
				memset(n->nodes, 0, sizeof(node*) << 4);
				nodes[key_real] = n;
				break;
			}
			else
			{
				if (n->key.length() >= klength)
				{
					if (n->key.length() == klength)
					{
						if (n->key == key)
						{
							n->height = height;
							n->index = index;
							break;
						}
					}
					else
					{
						if (i + 1 == klength_real)
						{
							std::swap(n->key, key);
							std::swap(n->height, height);
							std::swap(n->index, index);
							klength = key.length();
							klength_real = klength << 1;
						}
					}
				}
				nodes = n->nodes;
			}
		}
	}

	bool get(const std::string &key, unsigned long &height, unsigned long &index)
	{
		unsigned long klength = key.length(),
			klength_real = klength << 1;
		node **nodes = m_root;
		for (unsigned long i = 0; i < klength_real; ++i)
		{
			unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
			node *n = nodes[key_real];
			if (n == nullptr)
			{
				break;
			}
			else
			{
				if (n->key.length() == klength && n->key == key)
				{
					height = n->height;
					index = n->index;
					return true;
				}
			}
			nodes = n->nodes;
		}
		return false;
	}

	bool try_get(const std::string &key, unsigned long &height, unsigned long &index)
	{
		unsigned long klength = key.length(),
			klength_real = klength << 1;
		unsigned long height_last = 0;
		unsigned long index_last = 0;
		node **nodes = m_root;
		for (unsigned long i = 0; i < klength_real; ++i)
		{
			unsigned long key_real = 0xfUL & (i & 1UL ? key[i >> 1] : (key[i >> 1] >> 4));
			node *n = nodes[key_real];
			if (n == nullptr)
			{
				break;
			}
			else
			{
				if (n->key.length() == klength && n->key == key)
				{
					height = n->height;
					index = n->index;
					return true;
				}
				else
				{
					//特殊处理，获取能够匹配且高度最大的节点
					if ((klength << 1) >= n->height)
					{
						//获取两个字符串能够匹配的最大高度，因为只要不小于n->height就行，所以限制了循环次数
						//n->height必然是小于n->key的高度的，条件又限制了其不大于key的高度，所以循环不会溢出
						unsigned long h = 0;
						for (; h < n->height; ++h)
						{
							unsigned long key_real0 = 0xfUL & (h & 1UL ? n->key[h >> 1] : (n->key[h >> 1] >> 4));
							unsigned long key_real1 = 0xfUL & (h & 1UL ? key[h >> 1] : (key[h >> 1] >> 4));
							if (key_real0 != key_real1) break;
						}
						if (n->height == h)
						{
							height_last = n->height;
							index_last = n->index;
						}
					}
				}
			}
			nodes = n->nodes;
		}
		if (height_last)
		{
			height = height_last;
			index = index_last;
			return true;
		}
		return false;
	}

	void clear()
	{
		clear(m_root);
	}

private:

	void clear(node *nodes[16])
	{
		for (unsigned long i = 0; i < 16; ++i)
		{
			node *n = nodes[i];
			if (n)
			{
				clear(n->nodes);
			}
		}
	}

private:

	node *m_root[16];

};

#endif
