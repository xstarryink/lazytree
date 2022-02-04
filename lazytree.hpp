#pragma once
#include<type_traits>
#include<bits/unique_ptr.h>
#include<bits/move.h>
#include<bit>
#include<limits>
namespace{struct EmptyClass{};}
template<typename key_t,typename value_t=EmptyClass> requires std::is_integral_v<key_t>
class LazyTree
{
	struct Node;
	using ptrNode=std::unique_ptr<Node>;
	using ukey_t=std::make_unsigned_t<key_t>;
	static inline int highbit(key_t value)
	{
		return std::numeric_limits<key_t>::digits-std::__countl_zero(ukey_t(value))-1;
	}
	struct Node:value_t
	{
		key_t key;
		ptrNode son[2];
		Node(key_t key)
		:value_t(),key{key},son{}{}
	};
	static constexpr key_t invalidKey{};
	ptrNode root;
	Node *HalfwayInsert(const key_t key,key_t mask,ptrNode *cur_node)
	{
		for(;mask&&(*cur_node)&&((*cur_node)->key!=invalidKey)&&((*cur_node)->key!=key);mask>>=1)
		{
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		}
		if((!(*cur_node))||(((*cur_node)->key)==invalidKey))
		{
//			found an empty node on the way home
//			get lazy
			cur_node->reset(new Node(key));
			return cur_node->get();
		}
		else
		{
//			this is home and the node is occupied
//			expel the occupier
			if((*cur_node)->key!=key)
			{
				key_t occupier=(*cur_node)->key;
				(*cur_node)->key=key;
				dynamic_cast<value_t&>(*HalfwayInsert(occupier,key_t(1)<<highbit(occupier)>>highbit(key)>>1,cur_node))=std::move(dynamic_cast<value_t&>(**cur_node));
				return cur_node->get();
			}
			else
			{
				throw "Key already exist";
			}
		}
	}
	void eraseNode(ptrNode *node)
	{
		if(((*node)->son[0])||((*node)->son[1]))
		{
			(*node)->key=invalidKey;
		}
		else
		{
			node->reset();
		}
	}
public:
	LazyTree()noexcept=default;
	~LazyTree()=default;
	void clear()
	{
		root.reset(nullptr);
	}
	value_t& insert(key_t key)
	{
		++key;
		return *HalfwayInsert(key,key_t(1)<<highbit(key)>>1,&root);
	}
	value_t& at(key_t key)
	{
		++key;
		ptrNode *cur_node=&root;
		Node *emptyNode{nullptr};
		key_t mask=1<<highbit(key)>>1;
		for(;mask&&(*cur_node)&&((*cur_node)->key)!=key;mask>>=1)
		{
			if((*cur_node)->key==invalidKey)
			{
				emptyNode=cur_node->get();
			}
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		}
		if((*cur_node)&&((*cur_node)->key==key))
		{
//			found
			if(emptyNode)
			{
				emptyNode->key=key;
				dynamic_cast<value_t&>(*emptyNode)=std::move(dynamic_cast<value_t&>(**cur_node));
				eraseNode(cur_node);
				return dynamic_cast<value_t&>(*emptyNode);
			}
			return **cur_node;
		}
		else
		{
			throw "key quiried not found";
		}
	}
	const value_t& at(key_t key) const
	{
		++key;
		ptrNode const *cur_node=&root;
		key_t mask=key_t(1)<<highbit(key)>>1;
		for(;mask&&(*cur_node)&&((*cur_node)->key)!=key;mask>>=1)
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		if((*cur_node)&&((*cur_node)->key==key))
			return dynamic_cast<value_t&>(**cur_node);
		else
			throw "key not found";
	}
	bool has(key_t key) const noexcept
	{
		try
		{
			at(key);
			return true;
		}
		catch(const char*)
		{
			return false;
		}
	}
	bool count(key_t key) const noexcept
	{
		try
		{
			at(key);
			return true;
		}
		catch(const char*)
		{
			return false;
		}
	}
	void erase(key_t key)
	{
		++key;
		ptrNode *cur_node=&root;
		key_t mask=key_t(1)<<highbit(key)>>1;
		for(;mask&&(*cur_node)&&((*cur_node)->key)!=key;mask>>=1)
			cur_node=(*cur_node)->son+static_cast<bool>(key&mask);
		if((*cur_node)&&(*cur_node)->key==key)
			eraseNode(cur_node);
		else
			throw "key not found";
	}
};
